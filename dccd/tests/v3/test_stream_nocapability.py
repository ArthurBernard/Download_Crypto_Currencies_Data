"""Regression tests for B6: stream jobs with no live capability create zombie rows.

Three scenarios:
1. ``stream()`` raises ``NoCapability`` *before* inserting a run row → ``active_runs()`` empty.
2. ``_StreamWorker`` over a stub that raises ``NoCapability`` → stops permanently (no retry).
3. Backoff reset: after a long healthy run a subsequent failure resets delay to 5 s.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import patch

import pytest

from dccd.application.events import EventBus
from dccd.application.jobs import JobParams, JobSpec, JobTarget, Trigger
from dccd.application.operations import stream
from dccd.domain.capability import Capability
from dccd.domain.errors import NoCapability
from dccd.domain.symbol import Symbol
from dccd.domain.types import DataType
from dccd.sources.base import TradesLive
from dccd.storage.parquet import ParquetStore
from dccd.storage.runs_sqlite import RunsStore

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


class _NoCapabilityAdapter:
    """Adapter that declares *no* live capabilities for any data type."""

    exchange = "stub"

    def capabilities(self) -> list[Capability]:
        return []

    def capability_for(self, data_type: Any, transport: str, mode: str) -> None:
        return None


class _FakeReg:
    def __init__(self, adapter: Any) -> None:
        self._adapter = adapter

    def get(self, exchange: str) -> Any:
        return self._adapter


def _stream_spec(exchange: str = "stub", data_type: DataType = DataType.TRADES) -> JobSpec:
    target = JobTarget(
        exchange=exchange,
        symbol=Symbol.parse("BTC/USDT"),
        data_type=data_type,
    )
    return JobSpec(
        id=f"stream:{exchange}:BTC/USDT:{data_type.value}",
        operation="stream",
        target=target,
        trigger=Trigger(kind="supervised"),
        params=JobParams(),
        origin="config",
    )


# ---------------------------------------------------------------------------
# 1. operations.stream() — NoCapability before create_run
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_stream_no_capability_leaves_no_running_row(tmp_path):
    """stream() with an incapable adapter must raise and leave active_runs empty."""
    store = ParquetStore(str(tmp_path / "data"))
    runs = RunsStore(str(tmp_path / "runs.db"))

    spec = _stream_spec()
    reg = _FakeReg(_NoCapabilityAdapter())

    with pytest.raises(NoCapability):
        await stream(spec, registry=reg, store=store, runs_store=runs)

    assert runs.active_runs() == [], (
        "A run row was created before the capability check — zombie row bug (B6)"
    )


@pytest.mark.asyncio
async def test_stream_no_capability_raises_before_any_row(tmp_path):
    """Verify list_runs is also empty (not just active_runs) after the failure."""
    store = ParquetStore(str(tmp_path / "data"))
    runs = RunsStore(str(tmp_path / "runs.db"))

    spec = _stream_spec(data_type=DataType.ORDERBOOK)
    reg = _FakeReg(_NoCapabilityAdapter())

    with pytest.raises(NoCapability):
        await stream(spec, registry=reg, store=store, runs_store=runs)

    assert runs.list_runs(limit=10) == [], "No row should exist in runs.db at all"


# ---------------------------------------------------------------------------
# 2. _StreamWorker stops on NoCapability (no unbounded retry)
# ---------------------------------------------------------------------------


class _NoCapStream(TradesLive):
    """Trades source that always raises NoCapability on capability_for."""

    exchange = "stub"
    call_count = 0

    def capabilities(self) -> list[Capability]:
        return []

    def capability_for(self, data_type: Any, transport: str, mode: str) -> None:
        _NoCapStream.call_count += 1
        return None

    async def stream_trades(self, symbol: Any):
        # Should never be reached.
        raise AssertionError("stream_trades should not be called")
        # make this an async generator
        yield  # type: ignore[misc]  # pragma: no cover


@pytest.mark.asyncio
async def test_stream_worker_stops_on_no_capability(tmp_path):
    """_StreamWorker must stop permanently when stream() raises NoCapability."""
    from dccd.application.scheduler import _StreamWorker

    store = ParquetStore(str(tmp_path / "data"))
    runs = RunsStore(str(tmp_path / "runs.db"))
    events = EventBus()

    _NoCapStream.call_count = 0
    adapter = _NoCapStream()
    reg = _FakeReg(adapter)
    spec = _stream_spec()

    worker = _StreamWorker(spec, reg, store, runs, events)
    worker.start()

    # Give the event loop time to run _run_forever to completion.
    await asyncio.sleep(0.1)

    assert not worker.is_running, (
        "_StreamWorker should have stopped after NoCapability — permanent error"
    )
    # capability_for is called exactly once per stream() attempt; if the worker
    # were retrying, the count would grow with each sleep cycle.
    assert _NoCapStream.call_count == 1, (
        f"capability_for called {_NoCapStream.call_count} times — worker is retrying"
    )


@pytest.mark.asyncio
async def test_stream_worker_no_capability_no_zombie_row(tmp_path):
    """After _StreamWorker stops on NoCapability, runs.db must have no active rows."""
    from dccd.application.scheduler import _StreamWorker

    store = ParquetStore(str(tmp_path / "data"))
    runs = RunsStore(str(tmp_path / "runs.db"))
    events = EventBus()

    adapter = _NoCapStream()
    reg = _FakeReg(adapter)
    spec = _stream_spec()

    worker = _StreamWorker(spec, reg, store, runs, events)
    worker.start()
    await asyncio.sleep(0.1)

    assert runs.active_runs() == [], "No zombie 'running' row after permanent stop"


# ---------------------------------------------------------------------------
# 3. Backoff reset after a long healthy run
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_backoff_resets_after_long_healthy_run():
    """Delay resets to 5 s when the previous run lasted >= _HEALTHY_RUN_THRESHOLD_S.

    ``_run_forever`` does ``from dccd.application.operations import stream`` at
    call time so we must patch ``dccd.application.operations`` as a whole (swap
    the ``stream`` attribute on the already-imported module object) rather than
    via the dotted-string form.

    Sequence:
    - Call 1 fast fail:  monotonic returns 0.0, 0.0  → elapsed = 0 < 300 → delay stays 5, grows to 10.
    - Sleep 5 s.
    - Call 2 long run:   monotonic returns 0.0, 600.0 → elapsed = 600 >= 300 → reset to 5.
    - Sleep 5 s.
    - Call 3: stop_event set → loop exits without another sleep.
    """
    import dccd.application.operations as ops_mod
    from dccd.application.scheduler import _StreamWorker

    spec = _stream_spec()
    worker = _StreamWorker(spec, None, None, None, EventBus())  # type: ignore[arg-type]

    sleep_calls: list[float] = []
    call_count = 0

    # Provide enough values: 2 per failing call (t0 + elapsed measure).
    # Call 1: 0.0, 0.0.  Call 2: 0.0, 600.0.  Call 3: only t0 needed, then stop.
    mono_values = [0.0, 0.0, 0.0, 600.0, 0.0]
    mono_iter = iter(mono_values)

    async def fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    async def fake_stream(*args: Any, **kwargs: Any) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("fast transient failure")
        if call_count == 2:
            raise RuntimeError("failure after long healthy run")
        # Third attempt: signal stop so the loop exits cleanly.
        worker._stop_event.set()

    orig_stream = ops_mod.stream
    try:
        ops_mod.stream = fake_stream  # type: ignore[attr-defined]
        with (
            patch("dccd.application.scheduler.asyncio.sleep", side_effect=fake_sleep),
            patch("dccd.application.scheduler.time.monotonic", side_effect=mono_iter),
        ):
            await worker._run_forever()
    finally:
        ops_mod.stream = orig_stream  # type: ignore[attr-defined]

    # sleep_calls[0]: after fast fail, delay = 5 (threshold not reached).
    # sleep_calls[1]: after 600-s run, delay reset to 5 (not grown to 10).
    assert len(sleep_calls) >= 2, f"Expected >=2 sleep calls, got {sleep_calls}"
    assert sleep_calls[0] == pytest.approx(5.0), (
        f"First failure delay should be 5 s, got {sleep_calls[0]}"
    )
    assert sleep_calls[1] == pytest.approx(5.0), (
        f"After 600-s healthy run, delay should reset to 5 s, got {sleep_calls[1]}"
    )
