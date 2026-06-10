---
plan: perf-robustness/02-inventory-footer-metadata
kind: leaf
status: done
complexity: medium
depends: []
parallel: true
branch: fix/inventory-footer-metadata
pr: "#119"
---

# `ParquetStore` metadata from parquet footers + cache + off-thread API

## Goal

`ParquetStore.inventory()` (and `last_timestamp`, `missing_intervals`,
`_is_year_complete`) call `pl.read_parquet(f, columns=["TS"])` — materialising
the whole TS column of **every file in the store** — and run synchronously
inside async API endpoints, blocking the event loop that also drives WS
collection. Replace the column reads with parquet **footer metadata**
(row count + TS min/max statistics), add a per-file cache, and move the API
calls off-thread. Measured: `/api/inventory` took 100 s for 10 KB on a 50-file
store (under CPU starvation); cost grows ~365 files/year/pair for trades.

## Files to change

- `dccd/storage/parquet.py` —
  - new private helper `_file_stats(self, f: pathlib.Path) -> tuple[int, int | None, int | None]`
    returning `(rows, min_ts, max_ts)`:
    1. `stat()` the file; cache hit if `(st_mtime_ns, st_size)` unchanged —
       cache is a plain dict `self._stats_cache: dict[str, tuple[meta..]]` on
       the instance (the daemon holds one `ParquetStore`; CLI processes are
       short-lived, no invalidation API needed beyond mtime).
    2. on miss: `pyarrow.parquet.ParquetFile(f).metadata` → `num_rows`; TS
       min/max from per-row-group `statistics` of the `TS` column (find the
       column index by name via `schema_arrow`/`metadata.schema`); aggregate
       across row groups.
    3. **fallback**: if any row group lacks TS statistics (legacy writer),
       fall back to today's `pl.read_parquet(f, columns=["TS"])` for that
       file — correctness over speed.
  - `_ts_range(files)` → sums/aggregates via `_file_stats` (no column reads).
  - `last_timestamp(ds)` (line ~240) → per-file `_file_stats` max.
  - `missing_intervals(ds, …)` (line ~275) → file min/max via `_file_stats`.
  - `_is_year_complete(ds, year)` (line ~410) → needs only the row count:
    `_file_stats(file)[0]`.
  - `save()` need not touch the cache — mtime invalidation covers it.
- `dccd/interfaces/api/app.py` —
  - `GET /api/inventory`: `await asyncio.to_thread(store.inventory)`.
  - `GET /api/storage/sync`: replace its second full
    `_store(request).inventory()` scan with the same `to_thread` call (cheap
    now thanks to the cache; no separate bytes-only path needed).

## Steps

1. Implement `_file_stats` + cache in `ParquetStore` (import
   `pyarrow.parquet as pq` lazily inside the method, matching the file's
   existing style).
2. Convert the four call sites listed above; delete the now-unused direct
   `pl.read_parquet(..., columns=["TS"])` occurrences in those paths.
3. Wrap the two API endpoints in `asyncio.to_thread`.
4. `pytest`, `ruff check dccd/`, `mypy dccd/` (strict on storage? respect
   current config), `cd doc && make html` if docstrings changed.

## Tests

- `dccd/tests/v3/test_storage.py` / `test_storage_extended.py`:
  - write OHLC + trades + orderbook datasets via the public `save()` API; assert
    `inventory()` output (rows/min_ts/max_ts/files/bytes/expected/missing) is
    **identical** to the values the previous implementation produced (the
    existing assertions already encode them — they must pass unchanged).
  - cache behaviour: call `inventory()` twice; monkeypatch
    `pyarrow.parquet.ParquetFile` after the first call to raise — second call
    must succeed from cache. Then `save()` more rows into one dataset (mtime
    changes) and assert the new rows appear (restore the monkeypatch first).
  - fallback: craft a parquet file without statistics
    (`pq.write_table(..., write_statistics=False)`) and assert
    `inventory()`/`last_timestamp` still return correct min/max/rows.
  - `last_timestamp`, `missing_intervals`, `_is_year_complete` keep their
    existing tests green.

## Verification on real data

- `data-e2e` discipline on an isolated store: real backfill (e.g. Binance
  BTC/USDT 1h OHLC, 30 days + one trades hour), then compare `inventory()`
  JSON before/after the change (run old code via `git stash` or a checkout) —
  must be byte-identical. Time both: new implementation must be ≥ 10× faster
  on a store with ≥ 50 files (create extra files by backfilling several pairs
  if needed) and absolute time < 100 ms warm.
- `curl -w %{time_total}` on `GET /api/inventory` of a running `dccd ui`
  against that store: < 500 ms cold, < 50 ms warm.

## Closeout

- CHANGELOG (`Fixed`): "`ParquetStore` metadata (inventory, last timestamp, gap
  detection) no longer reads the full TS column of every file — it reads
  parquet footer statistics with a per-file mtime cache, and the API serves it
  off the event loop. `/api/inventory` on a populated store: seconds → tens of
  ms. (#NN)"
- ADR: footer statistics + mtime cache chosen over an explicit write-through
  cache or a manifest DB — zero new state to keep consistent, legacy files
  handled by per-file fallback.
- Status/roadmap: tick leaf 02 in `00-plan.md`.
