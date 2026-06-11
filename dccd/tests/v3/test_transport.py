"""Transport-layer tests — AsyncHTTPClient concurrency safety."""

import asyncio

import httpx
import pytest

from dccd.transport.http import AsyncHTTPClient


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
