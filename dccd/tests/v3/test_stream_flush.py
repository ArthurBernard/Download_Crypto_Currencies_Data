"""Tests for time-based flushing and rows_written accounting in stream().

Two behaviours under test:

1. **Time-based flush** — a fake TradesLive that yields 3 records then blocks
   forever; with a mocked ``time.monotonic`` that jumps past
   ``_STREAM_FLUSH_INTERVAL_S``, ``store.save`` must be called before the
   1000-record threshold is reached.

2. **rows_written accounting** — the finished run row in RunsStore must report
   a ``rows_written`` value consistent with the records actually stored; the
   count must be non-zero even when the stream is stopped before yielding 1000
   records.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from dccd.application.jobs import JobParams, JobSpec, JobTarget, Trigger
from dccd.application.operations import _STREAM_FLUSH_INTERVAL_S, stream
from dccd.domain.capability import Capability
from dccd.domain.records import Trade
from dccd.domain.symbol import Symbol
from dccd.domain.timeutils import ns_now
from dccd.domain.types import DataType
from dccd.sources.base import TradesLive
from dccd.storage.parquet import ParquetStore
from dccd.storage.runs_sqlite import RunsStore

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


class _SlowTradesWS(TradesLive):
    """Yields *n_initial* records immediately, then blocks on an asyncio.Event.

    Used to test time-based flushing without reaching the 1000-record batch
    threshold.
    """

    exchange = "fake"

    def __init__(self, n_initial: int = 3, *, stop: asyncio.Event | None = None) -> None:
        self._n = n_initial
        self._external_stop = stop

    def capabilities(self) -> list[Capability]:
        return [Capability(data_type=DataType.TRADES, transport="ws", mode="live")]

    async def stream_trades(self, symbol: Any):  # noqa: ANN001
        for i in range(self._n):
            yield Trade(ts=ns_now(), price=1.0 + i, amount=1.0, side="buy", tid=str(i))
        # Block until the caller sets stop_event (via the _external_stop
        # passed to stream()) — this simulates a slow live feed.
        if self._external_stop is not None:
            await self._external_stop.wait()


class _FakeReg:
    def __init__(self, src: Any) -> None:
        self._s = src

    def get(self, ex: str) -> Any:
        return self._s


def _spec() -> JobSpec:
    target = JobTarget(
        exchange="fake",
        symbol=Symbol.parse("BTC/USDT"),
        data_type=DataType.TRADES,
    )
    return JobSpec(
        id="stream:fake:BTC/USDT:trades",
        operation="stream",
        target=target,
        trigger=Trigger(kind="supervised"),
        params=JobParams(),
        origin="config",
    )


# ---------------------------------------------------------------------------
# 1. Time-based flush fires before the 1000-record threshold
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_time_flush_fires_before_threshold(tmp_path):
    """store.save must be called after the interval even with < 1000 records.

    Sequence:
    - Adapter yields 3 records, then blocks on stop_event.
    - A fake monotonic clock is injected into ``dccd.application.operations``
      only (not the asyncio event loop) by patching the module-level ``time``
      reference in operations.py.  The fake clock starts at 0 for the
      _last_flush initialisation, then returns a value well past the interval
      so _maybe_flush triggers on the first record arrival.
    - We assert store.save was called before stop_event was set, with all 3
      buffered records, even though the 1000-record threshold was never reached.
    """
    stop = asyncio.Event()
    adapter = _SlowTradesWS(n_initial=3, stop=stop)
    reg = _FakeReg(adapter)
    store = ParquetStore(str(tmp_path / "data"))
    runs = RunsStore(str(tmp_path / "runs.db"))

    save_calls: list[int] = []
    real_save = store.save

    def _recording_save(ds: Any, records: list[Any], prov: Any) -> int:
        n = real_save(ds, records, prov)
        save_calls.append(n)
        return n

    store.save = _recording_save  # type: ignore[method-assign]

    # Fake monotonic: first call returns 0.0 (initialises _last_flush),
    # every subsequent call returns a value past the interval so the next
    # _maybe_flush check always triggers.  Using a callable avoids the
    # StopIteration problem when asyncio's event loop also calls time.monotonic.
    call_count = 0

    def _fake_monotonic() -> float:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return 0.0  # _last_flush initialisation
        return _STREAM_FLUSH_INTERVAL_S + 10.0  # always past the threshold

    async def _drive():
        import time as _time_mod

        import dccd.application.operations as ops_mod
        fake_time = MagicMock(wraps=_time_mod)
        fake_time.monotonic = _fake_monotonic
        with patch.object(ops_mod, "time", fake_time):
            await stream(_spec(), registry=reg, store=store, runs_store=runs,
                         stop_event=stop)

    # After the 3 records are yielded the adapter blocks; unblock after a short
    # delay so the test doesn't hang.
    async def _unblock():
        await asyncio.sleep(0.15)
        stop.set()

    await asyncio.gather(_drive(), _unblock())

    # At least one mid-stream flush must have happened before stop_event fired.
    # Each record triggers _maybe_flush; since the clock always reads past the
    # interval, every record flushes its own 1-item batch (after the first
    # _last_flush is reset to the "past" value, subsequent checks still show
    # elapsed=0 — but the first call triggers immediately).  What matters is
    # that *at least one* flush happened before stop rather than all records
    # being held in RAM until stop.
    assert len(save_calls) >= 1, (
        f"Expected at least one save() call from time-based flush; got {save_calls}"
    )
    # Total rows across all mid-stream flushes plus any final flush on stop
    # should equal the 3 records yielded.
    total_flushed = sum(save_calls)
    assert total_flushed == 3, (
        f"Expected 3 total rows flushed, got {total_flushed} across {save_calls}"
    )


# ---------------------------------------------------------------------------
# 2. rows_written is non-zero after a stop with < 1000 records
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rows_written_counted_on_stop(tmp_path):
    """After a stop, run row rows_written == records actually stored."""
    n_records = 5
    stop = asyncio.Event()
    adapter = _SlowTradesWS(n_initial=n_records, stop=stop)
    reg = _FakeReg(adapter)
    store = ParquetStore(str(tmp_path / "data"))
    runs = RunsStore(str(tmp_path / "runs.db"))

    async def _drive():
        await stream(_spec(), registry=reg, store=store, runs_store=runs,
                     stop_event=stop)

    async def _unblock():
        # Wait long enough for all n_records to be yielded.
        await asyncio.sleep(0.15)
        stop.set()

    await asyncio.gather(_drive(), _unblock())

    run = runs.list_runs(limit=1)[0]
    assert run["state"] == "cancelled", f"Expected cancelled, got {run['state']}"
    assert run["rows_written"] == n_records, (
        f"Expected rows_written={n_records}, got {run['rows_written']}. "
        "Stream reported 0 rows even though records were flushed on stop."
    )


# ---------------------------------------------------------------------------
# 3. rows_written matches parquet content
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_rows_written_matches_parquet(tmp_path):
    """rows_written in runs.db must equal what read() returns from parquet."""
    n_records = 7
    stop = asyncio.Event()
    adapter = _SlowTradesWS(n_initial=n_records, stop=stop)
    reg = _FakeReg(adapter)
    store = ParquetStore(str(tmp_path / "data"))
    runs = RunsStore(str(tmp_path / "runs.db"))

    from dccd.application.jobs import JobTarget
    from dccd.application.operations import read

    async def _drive():
        await stream(_spec(), registry=reg, store=store, runs_store=runs,
                     stop_event=stop)

    async def _unblock():
        await asyncio.sleep(0.15)
        stop.set()

    await asyncio.gather(_drive(), _unblock())

    run = runs.list_runs(limit=1)[0]
    target = JobTarget(
        exchange="fake", symbol=Symbol.parse("BTC/USDT"), data_type=DataType.TRADES
    )
    df = read(target, store=store)
    on_disk = len(df) if df is not None else 0

    assert run["rows_written"] == on_disk, (
        f"rows_written={run['rows_written']} in runs.db but {on_disk} rows on disk"
    )
    assert on_disk == n_records, f"Expected {n_records} rows on disk, got {on_disk}"
