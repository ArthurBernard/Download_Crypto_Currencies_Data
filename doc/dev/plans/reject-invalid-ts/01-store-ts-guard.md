---
plan: reject-invalid-ts/01-store-ts-guard
kind: leaf
status: planned
complexity: medium
depends: []
parallel: false
branch: fix/reject-invalid-ts
pr: ""
---

# Reject bars with an invalid timestamp (TS<=0) at the storage write boundary

## Goal
Stop corrupt rows whose timestamp is null or `<= 0` (Unix epoch / lost time
field) from ever being written to the store. Such a row poisons gap detection —
a single `TS=0` bar makes `inventory()` report `min_ts = 1970-01-01`, inflating
`expected_rows`/`missing_rows` to a bogus ~89% gap. One central guard in
`ParquetStore.save()` covers every adapter and every data type.

## Context
Server audit 2026-06-19 found a corrupt Kraken OHLC bar
(`kraken/ohlc/BTC-USD/1m/1970.parquet`, one row: `TS=0, open=60882.4,
close=60867.9` — real BTC price, lost timestamp). It dragged the whole-store
OHLC missing aggregate to 32.7% (real ~1.5%). The bad row was removed manually
in prod; this leaf adds the code-side guard so it cannot recur. See
[[project-audit-2026-06-19]].

## Design decision
Guard **centrally in `ParquetStore.save()`**, not per-adapter. `save()` is the
single choke point every source's records pass through, so one filter defends
all exchanges and all data types (OHLC/trades/order book). A `TS <= 0` is always
invalid: all internal timestamps are ns UTC int64 and real crypto data starts
~2009, so epoch-or-earlier is unambiguously corrupt. We drop (not raise) and log
a warning, so one bad bar never aborts an otherwise-good page write.

## Files to change
- `dccd/storage/parquet.py` — in `save()`, immediately after
  `df = self._to_dataframe(ds, records)` and its empty-check (around line 287),
  filter out rows where `TS` is null or `<= 0`; if any were dropped, `logger.warning`
  the count and the `ds`; re-check for an emptied frame and return 0 if so. The
  filter must run **before** the `_period` column is derived (so the corrupt rows
  never reach `from_epoch`/period bucketing or `_merge`).

## Steps
1. In `ParquetStore.save()`, after the existing:
   ```python
   df = self._to_dataframe(ds, records)
   if len(df) == 0:
       return 0
   ```
   insert a guard:
   ```python
   # Reject bars with an invalid timestamp (null or <= 0). TS is ns UTC
   # int64; 0 is the Unix epoch (1970) — always corrupt for crypto market
   # data (real history starts ~2009). One such row poisons gap detection
   # (inventory min_ts → 1970, expected_rows balloons). Drop, don't raise,
   # so one bad bar can't abort a good page. Seen in prod: a Kraken OHLC bar
   # with a null time parsed to 0 (audit 2026-06-19).
   n_before = len(df)
   df = df.filter(pl.col("TS").is_not_null() & (pl.col("TS") > 0))
   dropped = n_before - len(df)
   if dropped:
       logger.warning("save(%s): dropped %d row(s) with invalid TS<=0", ds, dropped)
   if len(df) == 0:
       return 0
   ```
2. Confirm `logger` and `pl` are already imported in the module (they are) — no
   new imports.
3. `ruff check dccd/` and `mypy dccd/` clean.

## Tests
- `dccd/tests/v3/test_storage.py` (or `test_storage_extended.py`) — add a focused
  regression test, e.g. `test_save_rejects_nonpositive_ts`:
  - Build an OHLC dataset on a `tmp_path` store and `save()` a list of `OHLCBar`s
    mixing valid bars with one `ts=0` and one negative `ts=-1`.
  - Assert the return value counts only the valid rows, and that
    `store.load(ds)` / on-disk read contains **no** row with `TS <= 0`
    (`df["TS"].min() > 0`).
  - Assert no stray `1970.parquet` period file was created.
  - Optionally parametrise across a trades dataset to prove the guard is data-type
    agnostic.

## Verification on real data
Per the `data-e2e` discipline (a green unit suite is not enough):
1. On an **isolated** store path, run a real Kraken OHLC backfill for one pair
   (e.g. `BTC/USD`, span 60) for a recent window.
2. Read back what landed (`ParquetStore.load` or `inventory()`), and assert
   `min(TS) > 0` and there is no `…/1m/1970.parquet` partition.
3. Sanity-check coverage is otherwise unchanged vs. the same backfill without the
   guard (valid bars are untouched; only invalid ones are dropped).
4. Record the row counts / min_ts observed in the leaf PR description.

## Closeout
- CHANGELOG (`Fixed`): "Reject OHLC/trade/order-book rows with an invalid
  timestamp (`TS<=0`) at the storage write boundary, so a lost/epoch-0 timestamp
  can no longer be persisted or poison gap detection (#NN)."
- ADR (`doc/dev/03-decisions.md`): short entry — *central storage-boundary guard
  over per-adapter validation*; rationale: single choke point, data-type agnostic,
  invariant "TS is ns UTC ≥ real-data epoch". Drop-and-log, not raise.
- Status/roadmap: no existing roadmap line (ad-hoc fix surfaced by the
  2026-06-19 audit) — `/finish-task` adds the ADR + CHANGELOG; note the fix under
  `doc/dev/06-status.md` if it tracks data-integrity guards.
