---
plan: audit-fixes/03-stream-time-flush
kind: leaf
status: planned
complexity: medium
depends: [01]
parallel: false
branch: fix/stream-time-flush
pr: ""
---

# B2 — time-based flush for trades/OHLC streams + real `rows_written`

## Goal

Streams flush only every 1000 records: a quiet pair keeps hours of data in
RAM — lost on crash, invisible to inventory/freshness. Flush on a time
interval too. While touching every save site, count rows so stream runs stop
reporting `rows_written=0`.

## Files to change

- `dccd/application/operations.py` — `stream()` only:
  - module constant `_STREAM_FLUSH_INTERVAL_S = 60.0`.
  - in the TRADES and OHLC loops, track `last_flush = time.monotonic()`;
    flush when `len(batch) >= 1000` **or**
    `batch and time.monotonic() - last_flush >= _STREAM_FLUSH_INTERVAL_S`;
    update `last_flush` on every flush. (Note the check runs on record
    arrival — that is fine: with zero records there is nothing in RAM, so
    worst-case loss is bounded by the interval plus one inter-record gap.)
  - accumulate `rows_written += n` from every `store.save(...)` return
    (including the orderbook per-snapshot saves and the final flush) and
    pass it to **both** `finish_run` calls (`cancelled` and `failed` paths
    too — partial counts are still true counts).

## Steps

1. Refactor the two batched loops to a small local `maybe_flush()` closure
   (count + time conditions) so the logic exists once.
2. Thread the row counter through all finish paths.
3. `pytest`, `ruff`, `mypy`.

## Tests

- `dccd/tests/v3/test_application.py`: a fake `TradesLive` adapter yielding
  3 records then sleeping; with a mocked monotonic clock crossing the
  interval, assert `store.save` was called before the 1000-record threshold.
- Assert the finished run row's `rows_written` equals the records yielded
  (stop via `stop_event` after N records).

## Verification on real data

- Isolated store: stream binance BTC/USDT trades for ~3 min, **without**
  stopping it, and watch the dataset directory: the daily parquet must
  appear/grow within ~2× the flush interval (audit baseline: nothing on disk
  for 12 s+ until stop). Then stop; check the run row in runs.db reports a
  `rows_written` matching `read()`'s row count for the window (± dedup).

## Closeout

- CHANGELOG (`Fixed`): "Trades/OHLC streams flush to disk on a time interval
  (60 s) as well as on batch size, bounding crash loss on quiet pairs; stream
  runs now record their real `rows_written` (#NN)"
- ADR: none — parameters follow the audit recommendation.
- Status/roadmap: tick leaf in 00-plan.
