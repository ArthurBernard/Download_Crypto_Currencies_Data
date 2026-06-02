"""Core operations — backfill, stream, read, inventory."""

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


def _make_dataset_id(target: JobTarget) -> DatasetId:
    return DatasetId(
        exchange=target.exchange,
        symbol=target.symbol,
        data_type=target.data_type,
        span=target.span,
    )


async def backfill(
    spec: JobSpec,
    *,
    registry: SourceRegistry,
    store: ParquetStore,
    runs_store: RunsStore | None = None,
    events: RunEvents | None = None,
    stop_event: asyncio.Event | None = None,
) -> dict[str, Any]:
    """Backfill historical data from a source to the Parquet store.

    Parameters
    ----------
    spec : JobSpec
        Job specification (operation='backfill').
    registry : SourceRegistry
    store : ParquetStore
    runs_store : RunsStore or None
    events : RunEvents or None
    stop_event : asyncio.Event or None
        Set to cancel mid-run.

    Returns
    -------
    dict
        Summary with rows_written, start_ns, end_ns.
    """
    target = spec.target
    params = spec.params
    ds = _make_dataset_id(target)
    run_id = f"{spec.id}@{int(time.time() * NS)}"

    if runs_store:
        runs_store.create_run(
            run_id, spec.id, "backfill",
            target.exchange, str(target.symbol), target.data_type.value,
            started_at=ns_now(),
        )
    if events:
        events.log(f"Backfill start: {spec.id}")
        events.status("running")

    start_ns: int
    end_ns = ns_now()

    if params.start == "last":
        last = store.last_timestamp(ds)
        start_ns = last + 1 if last is not None else 0
    elif params.start == "origin":
        start_ns = 0
    else:
        start_ns = int(params.start)

    total_written = 0
    error_msg: str | None = None

    try:
        adapter = registry.get(target.exchange)

        if target.data_type == DataType.OHLC:
            if not isinstance(adapter, OHLCHistory):
                raise NoCapability(target.exchange, "ohlc", "historical")
            cap = adapter.capability_for(DataType.OHLC, "rest", "historical")
            if cap is None:
                raise NoCapability(target.exchange, "ohlc", "historical")

            from dccd.transport.paginate import paginate_ohlc

            bars: list[OHLCBar] = []
            async for bar in paginate_ohlc(
                adapter.fetch_ohlc_page.__func__.__get__(adapter) if hasattr(adapter.fetch_ohlc_page, '__func__') else adapter.fetch_ohlc_page,
                cap,
                start_ns,
                end_ns,
                target.span or 3600,
                emit_progress=lambda d, t: events and events.progress(d, t) or None,
            ):
                if stop_event and stop_event.is_set():
                    break
                bars.append(bar)
                if len(bars) >= 10000:
                    n = await asyncio.to_thread(
                        store.save, ds, bars,
                        Provenance(source=f"{target.exchange}:rest")
                    )
                    total_written += n
                    bars.clear()

            if bars:
                n = await asyncio.to_thread(
                    store.save, ds, bars,
                    Provenance(source=f"{target.exchange}:rest")
                )
                total_written += n

        elif target.data_type == DataType.TRADES:
            if not isinstance(adapter, TradesHistory):
                raise NoCapability(target.exchange, "trades", "historical")
            cap = adapter.capability_for(DataType.TRADES, "rest", "historical")
            if cap is None:
                raise NoCapability(target.exchange, "trades", "historical")

            from dccd.transport.paginate import paginate_trades

            batch: list[Trade] = []
            async for trade in paginate_trades(
                adapter.fetch_trades_page.__func__.__get__(adapter) if hasattr(adapter.fetch_trades_page, '__func__') else adapter.fetch_trades_page,
                cap,
                start_ns,
                end_ns,
                emit_progress=lambda d, t: events and events.progress(d, t) or None,
            ):
                if stop_event and stop_event.is_set():
                    break
                batch.append(trade)
                if len(batch) >= 10000:
                    n = await asyncio.to_thread(
                        store.save, ds, batch,
                        Provenance(source=f"{target.exchange}:rest")
                    )
                    total_written += n
                    batch.clear()

            if batch:
                n = await asyncio.to_thread(
                    store.save, ds, batch,
                    Provenance(source=f"{target.exchange}:rest")
                )
                total_written += n

        elif target.data_type == DataType.ORDERBOOK:
            if not isinstance(adapter, OrderBookSnapshotREST):
                raise NoCapability(target.exchange, "orderbook", "snapshot")
            depth = params.depth or 50
            snap = await adapter.fetch_orderbook(target.symbol, depth)
            n = await asyncio.to_thread(
                store.save, ds, [snap],
                Provenance(source=f"{target.exchange}:rest")
            )
            total_written += n

    except Exception as exc:
        error_msg = str(exc)
        logger.error("Backfill %s failed: %s", spec.id, exc)
        if events:
            events.log(f"ERROR: {exc}", level="error")
            events.status("failed")
        if runs_store:
            runs_store.finish_run(run_id, "failed", error=error_msg)
        return {"run_id": run_id, "rows_written": 0, "error": error_msg}

    state = "succeeded" if not (stop_event and stop_event.is_set()) else "cancelled"
    if events:
        events.log(f"Done: {total_written} rows written")
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
    """Stream live data continuously until stop_event is set."""
    from dccd.sources.base import OHLCLive, OrderBookLive, TradesLive

    target = spec.target
    params = spec.params
    ds = _make_dataset_id(target)
    run_id = f"{spec.id}@{int(time.time() * NS)}"

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
                    await asyncio.to_thread(
                        store.save, ds, batch,
                        Provenance(source=f"{target.exchange}:ws")
                    )
                    batch.clear()

        elif target.data_type == DataType.OHLC:
            if not isinstance(adapter, OHLCLive):
                raise NoCapability(target.exchange, "ohlc", "live")
            async for record in adapter.stream_ohlc(target.symbol, target.span or 3600):
                if stop_event and stop_event.is_set():
                    break
                batch.append(record)
                if len(batch) >= 1000:
                    await asyncio.to_thread(
                        store.save, ds, batch,
                        Provenance(source=f"{target.exchange}:ws")
                    )
                    batch.clear()

        elif target.data_type == DataType.ORDERBOOK:
            if not isinstance(adapter, OrderBookLive):
                raise NoCapability(target.exchange, "orderbook", "live")
            last_save = time.time()
            async for snap in adapter.stream_orderbook(target.symbol, params.depth or 50):
                if stop_event and stop_event.is_set():
                    break
                now = time.time()
                if now - last_save >= snapshot_interval:
                    await asyncio.to_thread(
                        store.save, ds, [snap],
                        Provenance(source=f"{target.exchange}:ws")
                    )
                    last_save = now

    except Exception as exc:
        if events:
            events.log(f"Stream error: {exc}", level="error")
            events.status("failed")
        if runs_store:
            runs_store.finish_run(run_id, "failed", error=str(exc))
        raise

    if batch:
        await asyncio.to_thread(
            store.save, ds, batch,
            Provenance(source=f"{target.exchange}:ws")
        )

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
    """Read stored data for a target."""
    ds = _make_dataset_id(target)
    return store.load(ds, start_ns, end_ns)


def inventory(*, store: ParquetStore) -> list[dict[str, Any]]:
    """List all available datasets."""
    return store.inventory()
