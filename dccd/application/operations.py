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
from dccd.domain.timeutils import NS, ns_now, ns_to_dt
from dccd.domain.types import DataType
from dccd.sources.base import OHLCHistory, OrderBookSnapshotREST, TradesHistory
from dccd.sources.registry import SourceRegistry
from dccd.storage.parquet import ParquetStore
from dccd.storage.runs_sqlite import RunsStore

__all__ = ["backfill", "stream", "read", "inventory"]

logger = logging.getLogger(__name__)

_FLUSH_BATCH = 10_000
# Default lookback per data type when start="last" and no data exists yet.
# Bounds the very first backfill so it can't silently run for millions of rows
# (trades) or from epoch 0. Deep history is opt-in via an explicit start date.
_DEFAULT_LOOKBACK_NS = {
    DataType.OHLC: 30 * 86400 * NS,      # ~720 1h bars / 43k 1m bars
    DataType.TRADES: 3600 * NS,          # 1 hour of trades
    DataType.ORDERBOOK: 3600 * NS,       # single snapshot anyway
}


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
    rows_so_far: int = 0,
    unit: str = "windows",
    at: int | None = None,
) -> None:
    """Emit progress to EventBus AND persist in RunsStore for polling.

    For ``unit="time"`` (backfills), ``done``/``total`` are nanoseconds covered
    of the requested window and ``at`` is the timestamp reached — which gives a
    real, smooth progress bar even for cursor-paginated trades (no page total).
    """
    if events is not None:
        events.progress(done, total)
    if runs_store is not None:
        prog: dict[str, Any] = {"done": done, "total": total, "unit": unit, "rows": rows_so_far}
        if at is not None:
            prog["at"] = at
        runs_store.update_progress(run_id, prog)


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
    batch: list[Any],
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
            # No data yet — pick a *bounded* default so "Backfill" with the
            # default start never silently triggers a multi-million-row run. The
            # safe window is data-type-aware: 30 days of OHLC is ~720 1h bars,
            # but 30 days of trades is tens of millions — so trades default to a
            # short recent window (use a custom start date for deep history).
            lookback = _DEFAULT_LOOKBACK_NS.get(target.data_type, 30 * 86400 * NS)
            start_ns = end_ns - lookback
            human = "1 hour" if target.data_type == DataType.TRADES else "30 days"
            _emit_log(events, runs_store, run_id,
                      f"No existing data — starting from {human} ago "
                      "(set a custom start date for more)")
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

    # Counts every item received from the paginator, including unflushed ones.
    _collected: list[int] = [0]

    # Progress is reported by *time covered* of the requested window, which gives
    # a real, smooth bar for both OHLC and cursor-paginated trades (the latter
    # have no page total). ``at`` is the timestamp reached. The window is read
    # from the *current* start_ns at call time — it may be clamped later
    # (history="recent"), and a precomputed window would leave the bar stuck
    # well below 100 % (the "looks frozen" symptom).
    def _emit_time(last_ts: int) -> None:
        win = max(1, end_ns - start_ns)
        done = min(win, max(0, last_ts - start_ns))
        _emit_progress(events, runs_store, run_id, done, win,
                       _collected[0], unit="time", at=last_ts)

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

            # Honour history="recent": these exchanges (e.g. Kraken, 720 bars)
            # only serve a recent window. Paginating deeper just refetches the
            # same recent bars — wasteful and misleading. Clamp + warn instead.
            if cap.history == "recent" and cap.max_per_request:
                earliest = end_ns - cap.max_per_request * span * NS
                if start_ns < earliest:
                    _emit_log(
                        events, runs_store, run_id,
                        f"{target.exchange} OHLC serves only the "
                        f"{cap.max_per_request} most recent bars; clamping start "
                        f"to {ns_to_dt(earliest).isoformat()}.",
                        level="warning",
                    )
                    start_ns = earliest

            sym = target.symbol

            async def _fetch_ohlc(s_ns: int, e_ns: int, limit: int) -> list[OHLCBar]:
                return await adapter.fetch_ohlc_page(sym, span, s_ns, e_ns, limit)

            bars: list[OHLCBar] = []
            async for bar in paginate_ohlc(_fetch_ohlc, cap, start_ns, end_ns, span):
                if stop_event and stop_event.is_set():
                    break
                bars.append(bar)
                _collected[0] += 1
                if _collected[0] % 200 == 0:
                    _emit_time(bar.ts)
                if len(bars) >= _FLUSH_BATCH:
                    total_written += await _flush(store, ds, bars, prov_src)

            total_written += await _flush(store, ds, bars, prov_src)
            _emit_time(end_ns)

        elif target.data_type == DataType.TRADES:
            if not isinstance(adapter, TradesHistory):
                raise NoCapability(target.exchange, "trades", "historical")
            cap = adapter.capability_for(DataType.TRADES, "rest", "historical")
            if cap is None:
                raise NoCapability(target.exchange, "trades", "historical")

            from dccd.transport.paginate import paginate_trades

            if cap.history == "recent":
                _emit_log(
                    events, runs_store, run_id,
                    f"{target.exchange} serves only recent trades; "
                    "deep history is unavailable.",
                    level="warning",
                )

            sym = target.symbol

            async def _fetch_trades(
                s_ns: int, e_ns: int, limit: int, cursor: str | None,
            ) -> tuple[list[Trade], str | None]:
                return await adapter.fetch_trades_page(sym, s_ns, e_ns, limit, cursor)

            batch: list[Trade] = []
            async for trade in paginate_trades(_fetch_trades, cap, start_ns, end_ns):
                if stop_event and stop_event.is_set():
                    break
                batch.append(trade)
                _collected[0] += 1
                if _collected[0] % 1000 == 0:
                    _emit_time(trade.ts)  # progress by time covered, not page count
                if len(batch) >= _FLUSH_BATCH:
                    total_written += await _flush(store, ds, batch, prov_src)

            total_written += await _flush(store, ds, batch, prov_src)
            if not (stop_event and stop_event.is_set()):
                _emit_time(end_ns)

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
    batch: list[Any] = []
    snapshot_interval = params.snapshot_interval or 60

    # Liveness heartbeat: emit a throttled sample (last price/quote) so the Live
    # UI can prove the stream is actually receiving data. Throttled to ~1/s so a
    # high-rate trade feed doesn't flood the SSE bus. Samples are NOT persisted.
    _last_sample: list[float] = [0.0]
    _SAMPLE_MIN_INTERVAL = 1.0

    def _emit_sample(
        ts: int,
        *,
        value: float | None = None,
        bid: float | None = None,
        ask: float | None = None,
    ) -> None:
        if events is None:
            return
        now_mono = time.monotonic()
        if now_mono - _last_sample[0] >= _SAMPLE_MIN_INTERVAL:
            _last_sample[0] = now_mono
            events.sample(ts, value=value, bid=bid, ask=ask)

    # Reject early if the adapter does not declare a live WS capability for this
    # data type — otherwise a missing/stub implementation would "run" forever
    # producing zero rows (the silent-empty-stream bug, D8).
    if adapter.capability_for(target.data_type, "ws", "live") is None:
        raise NoCapability(target.exchange, target.data_type.value, "live")

    try:
        if target.data_type == DataType.TRADES:
            if not isinstance(adapter, TradesLive):
                raise NoCapability(target.exchange, "trades", "live")
            async for record in adapter.stream_trades(target.symbol):
                if stop_event and stop_event.is_set():
                    break
                batch.append(record)
                _emit_sample(record.ts, value=record.price)
                if len(batch) >= 1000:
                    await asyncio.to_thread(store.save, ds, list(batch), Provenance(source=prov_src))
                    batch.clear()

        elif target.data_type == DataType.OHLC:
            if not isinstance(adapter, OHLCLive):
                raise NoCapability(target.exchange, "ohlc", "live")
            async for bar in adapter.stream_ohlc(target.symbol, target.span or 3600):
                if stop_event and stop_event.is_set():
                    break
                batch.append(bar)
                _emit_sample(bar.ts, value=bar.close)
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
                bid = snap.bids[0].price if snap.bids else None
                ask = snap.asks[0].price if snap.asks else None
                _emit_sample(snap.ts, bid=bid, ask=ask)
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
