"""Transport-layer tests — AsyncHTTPClient concurrency safety, WS reconnect."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest

from dccd.transport.http import AsyncHTTPClient
from dccd.transport.ws import _BACKOFF_FACTOR, _INITIAL_DELAY, WebSocketBase


@pytest.mark.asyncio
async def test_concurrent_context_does_not_close_client_early():
    """Two overlapping ``async with`` users share one client (review fix).

    Adapters share one AsyncHTTPClient; concurrent operations on the same
    exchange must not have the first finisher close the client out from under
    the other ("Cannot send a request, as the client has been closed").
    """
    http = AsyncHTTPClient()
    seen: list[bool] = []

    async def use(hold: float) -> None:
        async with http:
            await asyncio.sleep(hold)
            # While still inside the context, the client must be alive even if
            # another (shorter) user has already exited.
            seen.append(http._client is not None)

    await asyncio.gather(use(0.01), use(0.05))

    assert seen == [True, True]
    # Once the last user exits, the client is closed.
    assert http._client is None


@pytest.mark.asyncio
async def test_operation_level_context_one_pool_per_backfill(monkeypatch):
    """Operation-level ``async with adapter._http`` creates ONE httpx.AsyncClient.

    A backfill holds the adapter's HTTP client open for the whole operation so
    the inner per-page ``async with self._http`` calls become cheap ref-count
    increments rather than fresh TCP+TLS connections.  Simulate a 5-page
    backfill: the monkeypatched constructor must be called exactly once.
    """
    construction_count = 0
    real_init = httpx.AsyncClient.__init__

    def counting_init(self: httpx.AsyncClient, **kwargs: object) -> None:
        nonlocal construction_count
        construction_count += 1
        real_init(self, **kwargs)

    monkeypatch.setattr(httpx.AsyncClient, "__init__", counting_init)

    http = AsyncHTTPClient()

    async def simulate_five_pages() -> None:
        # Outer context: operation-level hold (what backfill() now does)
        async with http:
            for _ in range(5):
                # Inner context: per-page call (what fetch_*_page does)
                async with http:
                    await asyncio.sleep(0)  # stand-in for an HTTP request

    await simulate_five_pages()

    assert construction_count == 1, (
        f"Expected 1 httpx.AsyncClient instantiation; got {construction_count}. "
        "Each page must reuse the same pool, not open a new connection."
    )
    assert http._client is None  # pool closed after the outer context exited


@pytest.mark.asyncio
async def test_concurrent_operations_share_pool_no_closed_client_error(monkeypatch):
    """Concurrent backfills on the same adapter never see 'client has been closed'.

    When two backfills run concurrently on the same exchange the ref-count keeps
    the shared pool alive until both finish.  Neither should ever encounter a
    closed client while still inside the operation context.
    """
    construction_count = 0
    real_init = httpx.AsyncClient.__init__

    def counting_init(self: httpx.AsyncClient, **kwargs: object) -> None:
        nonlocal construction_count
        construction_count += 1
        real_init(self, **kwargs)

    monkeypatch.setattr(httpx.AsyncClient, "__init__", counting_init)

    http = AsyncHTTPClient()
    errors: list[str] = []

    async def simulate_backfill(pages: int, hold: float) -> None:
        async with http:  # operation-level hold
            for _ in range(pages):
                async with http:  # per-page inner context
                    if http._client is None:
                        errors.append("client was None inside page context")
                    await asyncio.sleep(hold)

    # Two concurrent backfills: one short (3 pages), one long (5 pages)
    await asyncio.gather(
        simulate_backfill(3, 0.01),
        simulate_backfill(5, 0.005),
    )

    assert errors == [], f"Closed-client errors during concurrent operations: {errors}"
    # Only one pool constructed total (both operations shared it)
    assert construction_count == 1
    assert http._client is None  # closed when both finished


@pytest.mark.asyncio
async def test_client_context_manager_opens_and_closes_pools(tmp_path, monkeypatch):
    """``async with Client()`` opens one pool per adapter and closes all on exit.

    The public Client facade enters every adapter's HTTP client on ``__aenter__``
    and exits them on ``__aexit__``.  After exit, all adapter pools must be
    closed; re-entering must work cleanly (reopens).
    """
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "noconfig"))

    from dccd import Client

    async with Client() as c:
        # Every adapter with an HTTP client must have its pool open
        for adapter in c._registry.adapters.values():
            http = adapter.http_client
            if http is not None:
                assert http._depth >= 1, (
                    f"{adapter.exchange} http._depth={http._depth} expected ≥ 1 "
                    "inside Client context"
                )
                assert http._client is not None, (
                    f"{adapter.exchange} httpx.AsyncClient is None inside context"
                )

    # After exit: all pools closed, depth reset
    assert c._registry is None

    # Re-entering must work cleanly without leaking state from the first run
    async with Client() as c2:
        for adapter in c2._registry.adapters.values():
            http = adapter.http_client
            if http is not None:
                assert http._client is not None

# ---------------------------------------------------------------------------
# Helpers for WebSocketBase reconnect tests
# ---------------------------------------------------------------------------

def _make_fake_ws(*frame_batches: list[str], fail_first: bool = False) -> Any:
    """Build a fake WS context manager that yields frames in batches.

    Parameters
    ----------
    frame_batches:
        Each element is a list of raw frames yielded by one ``connect`` call.
    fail_first:
        If True the very first connect attempt raises ``ConnectionError`` before
        yielding any frames.
    """
    call_count = 0

    async def _aiter_frames(frames: list[str]) -> AsyncIterator[str]:
        for f in frames:
            yield f

    @asynccontextmanager
    async def _fake_connect(url: str, **kwargs: Any) -> AsyncIterator[Any]:
        nonlocal call_count
        idx = call_count
        call_count += 1
        if fail_first and idx == 0:
            raise ConnectionError("simulated drop on first connect")
        ws = MagicMock()
        ws.__aiter__ = lambda self: _aiter_frames(frame_batches[min(idx, len(frame_batches) - 1)])
        yield ws

    return _fake_connect


# ---------------------------------------------------------------------------
# WebSocketBase.stream_raw() tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_ws_stream_raw_yields_frames(monkeypatch: pytest.MonkeyPatch) -> None:
    """stream_raw() yields all frames from a stable connection."""
    frames = ["frame1", "frame2", "frame3"]
    fake_connect = _make_fake_ws(frames)

    ws = WebSocketBase("ws://fake")
    collected: list[str] = []

    async def _collect() -> None:
        async for raw in ws.stream_raw():
            collected.append(raw)
            if len(collected) >= 3:
                ws.stop()

    with patch("websockets.connect", fake_connect):
        await _collect()

    assert collected == frames


@pytest.mark.asyncio
async def test_ws_stream_raw_reconnects_after_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """stream_raw() reconnects after a connection error with exponential backoff.

    The test patches asyncio.sleep so no real delay occurs, and verifies:
    - at least one reconnect happens
    - a frame from the second connection is received
    """
    slept: list[float] = []

    async def fake_sleep(delay: float) -> None:
        slept.append(delay)

    # First connect raises, second yields one frame then ws.stop() fires.
    fake_connect = _make_fake_ws([], ["after_reconnect"], fail_first=True)

    ws = WebSocketBase("ws://fake")
    collected: list[str] = []

    async def _collect() -> None:
        async for raw in ws.stream_raw():
            collected.append(raw)
            ws.stop()

    with (
        patch("websockets.connect",fake_connect),
        patch("asyncio.sleep", fake_sleep),
    ):
        await _collect()

    # At least one sleep (backoff) was recorded
    assert len(slept) >= 1
    # Initial delay is _INITIAL_DELAY
    assert slept[0] == pytest.approx(_INITIAL_DELAY)
    # A frame from the second connection was received
    assert "after_reconnect" in collected


@pytest.mark.asyncio
async def test_ws_stream_raw_backoff_doubles(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exponential backoff: each successive sleep doubles the previous one."""
    slept: list[float] = []

    async def fake_sleep(delay: float) -> None:
        slept.append(delay)

    # Three failing connections then a success.
    fail_count = 0

    @asynccontextmanager
    async def _failing_then_ok(url: str, **kwargs: Any) -> AsyncIterator[Any]:
        nonlocal fail_count
        attempt = fail_count
        fail_count += 1
        if attempt < 3:
            raise ConnectionError(f"drop #{attempt}")
        ws_mock = MagicMock()
        ws_mock.__aiter__ = lambda self: _one_frame()
        yield ws_mock

    async def _one_frame() -> AsyncIterator[str]:
        yield "ok"

    ws = WebSocketBase("ws://fake")
    collected: list[str] = []

    async def _collect() -> None:
        async for raw in ws.stream_raw():
            collected.append(raw)
            ws.stop()

    with (
        patch("websockets.connect",_failing_then_ok),
        patch("asyncio.sleep", fake_sleep),
    ):
        await _collect()

    # Three sleep calls for three failures
    assert len(slept) == 3
    assert slept[0] == pytest.approx(_INITIAL_DELAY)
    assert slept[1] == pytest.approx(_INITIAL_DELAY * _BACKOFF_FACTOR)
    assert slept[2] == pytest.approx(_INITIAL_DELAY * _BACKOFF_FACTOR**2)
    assert "ok" in collected


@pytest.mark.asyncio
async def test_ws_stream_raw_delay_resets_on_success(monkeypatch: pytest.MonkeyPatch) -> None:
    """After a successful connection the delay resets to _INITIAL_DELAY."""
    slept: list[float] = []

    async def fake_sleep(delay: float) -> None:
        slept.append(delay)

    call_count = 0

    @asynccontextmanager
    async def _alternating(url: str, **kwargs: Any) -> AsyncIterator[Any]:
        """Fail, succeed (2 frames), fail, succeed (stop)."""
        nonlocal call_count
        idx = call_count
        call_count += 1
        if idx == 0:
            raise ConnectionError("first drop")
        ws_mock = MagicMock()
        if idx == 1:
            ws_mock.__aiter__ = lambda self: _two_frames()
        elif idx == 2:
            raise ConnectionError("second drop")
        else:
            ws_mock.__aiter__ = lambda self: _stop_frame()
        yield ws_mock

    async def _two_frames() -> AsyncIterator[str]:
        yield "a"
        yield "b"

    async def _stop_frame() -> AsyncIterator[str]:
        yield "done"

    ws = WebSocketBase("ws://fake")
    collected: list[str] = []

    async def _collect() -> None:
        async for raw in ws.stream_raw():
            collected.append(raw)
            if raw == "done":
                ws.stop()

    with (
        patch("websockets.connect",_alternating),
        patch("asyncio.sleep", fake_sleep),
    ):
        await _collect()

    # Two sleeps: one after first drop, one after second drop
    assert len(slept) == 2
    # Both should be _INITIAL_DELAY because the middle connection succeeded
    # and reset the delay.
    assert slept[0] == pytest.approx(_INITIAL_DELAY)
    assert slept[1] == pytest.approx(_INITIAL_DELAY)


@pytest.mark.asyncio
async def test_ws_stop_prevents_reconnect(monkeypatch: pytest.MonkeyPatch) -> None:
    """After stop() is called mid-stream, no reconnect happens."""
    slept: list[float] = []

    async def fake_sleep(delay: float) -> None:
        slept.append(delay)

    @asynccontextmanager
    async def _always_fail(url: str, **kwargs: Any) -> AsyncIterator[Any]:
        raise ConnectionError("drop")
        yield  # type: ignore[misc]

    ws = WebSocketBase("ws://fake")
    ws.stop()  # stop before we even start

    collected: list[str] = []
    with (
        patch("websockets.connect",_always_fail),
        patch("asyncio.sleep", fake_sleep),
    ):
        async for raw in ws.stream_raw():
            collected.append(raw)

    assert collected == []
    assert slept == []


@pytest.mark.asyncio
async def test_ws_cancelled_error_exits_cleanly(monkeypatch: pytest.MonkeyPatch) -> None:
    """CancelledError propagates out without triggering a reconnect."""
    slept: list[float] = []

    async def fake_sleep(delay: float) -> None:
        slept.append(delay)

    @asynccontextmanager
    async def _cancel_on_connect(url: str, **kwargs: Any) -> AsyncIterator[Any]:
        raise asyncio.CancelledError()
        yield  # type: ignore[misc]

    ws = WebSocketBase("ws://fake")
    collected: list[str] = []

    with (
        patch("websockets.connect",_cancel_on_connect),
        patch("asyncio.sleep", fake_sleep),
    ):
        async for raw in ws.stream_raw():
            collected.append(raw)

    assert collected == []
    assert slept == []


@pytest.mark.asyncio
async def test_ws_on_connect_called_on_each_reconnect(monkeypatch: pytest.MonkeyPatch) -> None:
    """on_connect() must be called after every successful (re)connect."""
    connect_calls: list[int] = []

    class _CountingWS(WebSocketBase):
        async def on_connect(self, ws: Any) -> None:
            connect_calls.append(1)

    call_count = 0

    @asynccontextmanager
    async def _fail_then_ok(url: str, **kwargs: Any) -> AsyncIterator[Any]:
        nonlocal call_count
        idx = call_count
        call_count += 1
        if idx == 0:
            raise ConnectionError("first drop")
        ws_mock = MagicMock()
        ws_mock.__aiter__ = lambda self: _one_frame()
        yield ws_mock

    async def _one_frame() -> AsyncIterator[str]:
        yield "frame"

    async def fake_sleep(delay: float) -> None:
        pass

    ws = _CountingWS("ws://fake")

    async def _collect() -> None:
        async for raw in ws.stream_raw():
            ws.stop()

    with (
        patch("websockets.connect",_fail_then_ok),
        patch("asyncio.sleep", fake_sleep),
    ):
        await _collect()

    # on_connect should have been called once (only the second attempt succeeds).
    assert len(connect_calls) == 1


@pytest.mark.asyncio
async def test_ws_stream_delegates_to_parse_message() -> None:
    """stream() feeds each raw frame to parse_message and yields the results."""
    frames_seen: list[str] = []

    class _EchoWS(WebSocketBase):
        async def parse_message(self, raw: str | bytes) -> AsyncIterator[str]:
            frames_seen.append(str(raw))
            yield f"parsed:{raw}"

    @asynccontextmanager
    async def _two_frames(url: str, **kwargs: Any) -> AsyncIterator[Any]:
        ws_mock = MagicMock()
        ws_mock.__aiter__ = lambda self: _iter()
        yield ws_mock

    async def _iter() -> AsyncIterator[str]:
        yield "msg1"
        yield "msg2"

    ws = _EchoWS("ws://fake")
    collected: list[str] = []

    async def _collect() -> None:
        async for record in ws.stream():
            collected.append(record)
            if len(collected) >= 2:
                ws.stop()

    with patch("websockets.connect",_two_frames):
        await _collect()

    assert collected == ["parsed:msg1", "parsed:msg2"]

