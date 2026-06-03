"""Core operations — backfill, stream, read, inventory.

All public functions are async and accept keyword-only infrastructure arguments
(registry, store, events, …) to keep call sites explicit and testable.

**Key contract for paginators**: each ``fetch_page`` passed to
``paginate_ohlc`` / ``paginate_trades`` must be a closure with ``symbol``
(and ``span`` for OHLC) already bound:

    async def _fetch(start_ns, end_ns, limit):
        return await adapter.fetch_ohlc_page(symbol, span, start_ns, end_ns, limit)
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from dccd.application.events import RunEvents
from dccd.application.jobs import JobSpec, JobTarget
from dccd.domain.dataset import DatasetId, Provenance
from dccd.domain.errors import NoCapability
from dccd.domain.records import OHLCBar, Trade
from dccd.domain.timeutils import NS, ns_now
from dccd.domain.types import DataType
from dccd.sources.base import OHLCHistory, OrderBookSnapshotREST, TradesHistory
from dccd.sources.registry import SourceRegistry
from dccd.storage.parquet import ParquetStore
from dccd.storage.runs_sqlite import RunsStore

__all__ = ["backfill", "stream", "read", "inventory"]

logger = logging.getLogger(__name__)

_FLUSH_BATCH = 10_000
# Default lookback when no previous data exists and start="last".
# Avoids a 56-year paginator run from epoch 0.
_DEFAULT_LOOKBACK_NS = 30 * 86400 * NS  # 30 days


def _make_dataset_id(target: JobTarget) -> DatasetId:
    return DatasetId(
        exchange=target.exchange,
        symbol=target.symbol,
        data_type=target.data_type,
        span=target.span,
    )


def _emit_progress(
    events: RunEvents | None,
    runs_store: RunsStore | None,
    run_id: str,
    done: int,
    total: int,
) -> None:
    """Emit progress to EventBus AND persist in RunsStore for polling."""
    if events is not None:
        events.progress(done, total)
    if runs_store is not None:
        runs_store.update_progress(run_id, {"done": done, "total": total, "unit": "windows"})


def _emit_log(
    events: RunEvents | None,
    runs_store: RunsStore | None,
    run_id: str,
    msg: str,
    level: str = "info",
) -> None:
    """Emit a log line to EventBus SSE AND persist it in RunsStore log_tail."""
    if events is not None:
        events.log(msg, level=level)
    if runs_store is not None:
        runs_store.append_log(run_id, f"[{level.upper()}] {msg}")


async def _flush(
    store: ParquetStore,
    ds: DatasetId,
    batch: list,
    source: str,
) -> int:
    if not batch:
        return 0
    n = await asyncio.to_thread(store.save, ds, list(batch), Provenance(source=source))
    batch.clear()
    return n


async def backfill(
    spec: JobSpec,
    *,
    registry: SourceRegistry,
    store: ParquetStore,
    runs_store: RunsStore | None = None,
    events: RunEvents | None = None,
    stop_event: asyncio.Event | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Backfill historical data from a source to the Parquet store.

    Parameters
    ----------
    spec : JobSpec
        Job specification (``operation='backfill'``).
        ``spec.params.start`` controls the start point:

        - ``'last'``: resume from the last stored timestamp.
          If no data exists yet, defaults to 30 days ago to avoid a
          multi-decade paginator run from epoch 0.
        - ``'origin'``: start from the exchange's earliest available data
          (timestamp 0 — many pages will be empty before the exchange's
          launch date).
        - An **ISO-8601 date string** (``'2024-01-01'``) or a nanosecond
          integer: explicit start timestamp.

    registry : SourceRegistry
    store : ParquetStore
    runs_store : RunsStore or None
    events : RunEvents or None
    stop_event : asyncio.Event or None
        Set externally to cancel mid-run cleanly.
    run_id : str or None
        Override the auto-generated run ID (used by the API endpoint so
        the polling URL matches what is stored in RunsStore).

    Returns
    -------
    dict
        ``{'run_id', 'rows_written', 'start_ns', 'end_ns'}`` on success;
        ``{'run_id', 'rows_written', 'error'}`` on failure.
    """
    target = spec.target
    params = spec.params
    ds = _make_dataset_id(target)
    if run_id is None:
        run_id = f"{spec.id}@{int(time.time() * NS)}"

    if runs_store:
        runs_store.create_run(
            run_id, spec.id, "backfill",
            target.exchange, str(target.symbol), target.data_type.value,
            started_at=ns_now(),
        )
    _emit_log(events, runs_store, run_id, f"Backfill start: {spec.id}")
    if events:
        events.status("running")

    end_ns = ns_now()

    if params.start == "last":
        last = store.last_timestamp(ds)
        if last is not None:
            start_ns: int = last + 1
        else:
            # No data yet — start 30 days back instead of epoch 0.
            start_ns = end_ns - _DEFAULT_LOOKBACK_NS
            _emit_log(events, runs_store, run_id, "No existing data — starting from 30 days ago")
    elif params.start == "origin":
        start_ns = 0
    else:
        # ISO date string or nanosecond integer
        raw = str(params.start)
        if raw.isdigit():
            start_ns = int(raw)
        else:
            from dccd.domain.timeutils import str_to_ns
            start_ns = str_to_ns(raw[:10], form="%Y-%m-%d", tz="UTC")

    total_written = 0
    prov_src = f"{target.exchange}:rest"

    # Progress callback that updates both the EventBus and the RunsStore.
    def _on_progress(done: int, total: int) -> None:
        _emit_progress(events, runs_store, run_id, done, total)

    try:
        adapter = registry.get(target.exchange)

        if target.data_type == DataType.OHLC:
            if not isinstance(adapter, OHLCHistory):
                raise NoCapability(target.exchange, "ohlc", "historical")
            cap = adapter.capability_for(DataType.OHLC, "rest", "historical")
            if cap is None:
                raise NoCapability(target.exchange, "ohlc", "historical")

            from dccd.transport.paginate import paginate_ohlc

            span = target.span or 3600
            if cap.spans and span not in cap.spans:
                raise ValueError(
                    f"Span {span}s not supported by {target.exchange}. "
                    f"Supported spans: {sorted(cap.spans)}"
                )
            sym = target.symbol

            async def _fetch_ohlc(s_ns: int, e_ns: int, limit: int) -> list[OHLCBar]:
                return await adapter.fetch_ohlc_page(sym, span, s_ns, e_ns, limit)  # type: ignore[union-attr]

            bars: list[OHLCBar] = []
            async for bar in paginate_ohlc(
                _fetch_ohlc, cap, start_ns, end_ns, span,
                emit_progress=_on_progress,
            ):
                if stop_event and stop_event.is_set():
                    break
                bars.append(bar)
                if len(bars) >= _FLUSH_BATCH:
                    total_written += await _flush(store, ds, bars, prov_src)

            total_written += await _flush(store, ds, bars, prov_src)

        elif target.data_type == DataType.TRADES:
            if not isinstance(adapter, TradesHistory):
                raise NoCapability(target.exchange, "trades", "historical")
            cap = adapter.capability_for(DataType.TRADES, "rest", "historical")
            if cap is None:
                raise NoCapability(target.exchange, "trades", "historical")

            from dccd.transport.paginate import paginate_trades

            sym = target.symbol

            async def _fetch_trades(s_ns: int, e_ns: int, limit: int) -> list[Trade]:
                return await adapter.fetch_trades_page(sym, s_ns, e_ns, limit)  # type: ignore[union-attr]

            batch: list[Trade] = []
            async for trade in paginate_trades(
                _fetch_trades, cap, start_ns, end_ns,
                emit_progress=_on_progress,
            ):
                if stop_event and stop_event.is_set():
                    break
                batch.append(trade)
                if len(batch) >= _FLUSH_BATCH:
                    total_written += await _flush(store, ds, batch, prov_src)

            total_written += await _flush(store, ds, batch, prov_src)

        elif target.data_type == DataType.ORDERBOOK:
            if not isinstance(adapter, OrderBookSnapshotREST):
                raise NoCapability(target.exchange, "orderbook", "snapshot")
            depth = params.depth or 50
            snap = await adapter.fetch_orderbook(target.symbol, depth)
            total_written += await _flush(store, ds, [snap], prov_src)

    except Exception as exc:
        error_msg = str(exc)
        logger.error("Backfill %s failed: %s", spec.id, exc)
        _emit_log(events, runs_store, run_id, f"ERROR: {exc}", level="error")
        if events:
            events.status("failed")
        if runs_store:
            runs_store.finish_run(run_id, "failed", error=error_msg)
        return {"run_id": run_id, "rows_written": 0, "error": error_msg}

    state = "cancelled" if (stop_event and stop_event.is_set()) else "succeeded"
    _emit_log(events, runs_store, run_id, f"Done: {total_written} rows written")
    if events:
        events.status(state)
    if runs_store:
        runs_store.finish_run(run_id, state, rows_written=total_written)

    return {"run_id": run_id, "rows_written": total_written, "start_ns": start_ns, "end_ns": end_ns}


async def stream(
    spec: JobSpec,
    *,
    registry: SourceRegistry,
    store: ParquetStore,
    runs_store: RunsStore | None = None,
    events: RunEvents | None = None,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Stream live data continuously until *stop_event* is set."""
    from dccd.sources.base import OHLCLive, OrderBookLive, TradesLive

    target = spec.target
    params = spec.params
    ds = _make_dataset_id(target)
    run_id = f"{spec.id}@{int(time.time() * NS)}"
    prov_src = f"{target.exchange}:ws"

    if runs_store:
        runs_store.create_run(
            run_id, spec.id, "stream",
            target.exchange, str(target.symbol), target.data_type.value,
            started_at=ns_now(),
        )
    if events:
        events.log(f"Stream start: {spec.id}")
        events.status("running")

    adapter = registry.get(target.exchange)
    batch: list = []
    snapshot_interval = params.snapshot_interval or 60

    try:
        if target.data_type == DataType.TRADES:
            if not isinstance(adapter, TradesLive):
                raise NoCapability(target.exchange, "trades", "live")
            async for record in adapter.stream_trades(target.symbol):
                if stop_event and stop_event.is_set():
                    break
                batch.append(record)
                if len(batch) >= 1000:
                    await asyncio.to_thread(store.save, ds, list(batch), Provenance(source=prov_src))
                    batch.clear()

        elif target.data_type == DataType.OHLC:
            if not isinstance(adapter, OHLCLive):
                raise NoCapability(target.exchange, "ohlc", "live")
            async for record in adapter.stream_ohlc(target.symbol, target.span or 3600):
                if stop_event and stop_event.is_set():
                    break
                batch.append(record)
                if len(batch) >= 1000:
                    await asyncio.to_thread(store.save, ds, list(batch), Provenance(source=prov_src))
                    batch.clear()

        elif target.data_type == DataType.ORDERBOOK:
            if not isinstance(adapter, OrderBookLive):
                raise NoCapability(target.exchange, "orderbook", "live")
            last_save = time.time()
            async for snap in adapter.stream_orderbook(target.symbol, params.depth or 50):
                if stop_event and stop_event.is_set():
                    break
                if time.time() - last_save >= snapshot_interval:
                    await asyncio.to_thread(store.save, ds, [snap], Provenance(source=prov_src))
                    last_save = time.time()

    except Exception as exc:
        if events:
            events.log(f"Stream error: {exc}", level="error")
            events.status("failed")
        if runs_store:
            runs_store.finish_run(run_id, "failed", error=str(exc))
        raise

    if batch:
        await asyncio.to_thread(store.save, ds, batch, Provenance(source=prov_src))

    if events:
        events.status("cancelled")
    if runs_store:
        runs_store.finish_run(run_id, "cancelled")


def read(
    target: JobTarget,
    *,
    store: ParquetStore,
    start_ns: int | None = None,
    end_ns: int | None = None,
) -> Any:
    """Read stored data for *target* in the given nanosecond range."""
    ds = _make_dataset_id(target)
    return store.load(ds, start_ns, end_ns)


def inventory(*, store: ParquetStore) -> list[dict[str, Any]]:
    """Return a list of dataset descriptors for all stored data."""
    return store.inventory()
