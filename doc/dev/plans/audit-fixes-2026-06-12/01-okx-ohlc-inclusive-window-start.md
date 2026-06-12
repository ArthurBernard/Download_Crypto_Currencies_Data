---
plan: audit-fixes-2026-06-12/01-okx-ohlc-inclusive-window-start
kind: leaf
status: planned
complexity: medium
depends: []
parallel: false
branch: fix/okx-ohlc-boundary-bar
pr: ""
---

# OKX OHLC: make the pagination-window start inclusive

## Goal

OKX v5 `before`/`after` cursors are **exclusive** (`before`: records *newer than*
ts; `after`: records *earlier than* ts). `fetch_ohlc_page` passes
`before = start_ms`, so the bar exactly at every forward-pagination window start
is silently dropped — one missing 1m bar per 100-bar page. Pass
`before = start_ms - 1` so the requested window start is included.

## Files to change

- `dccd/sources/okx.py` — `OKX.fetch_ohlc_page` (~line 108): change
  `"before": str(start_ns // 1_000_000)` to
  `"before": str(start_ns // 1_000_000 - 1)`, with a one-line comment stating
  that OKX `before`/`after` are exclusive and that without the `- 1` the bar at
  every page boundary is lost. Do **not** touch `fetch_trades_page` — its
  backward `after` cursor *relies* on exclusivity to not re-fetch the cursor row.
- `dccd/tests/v3/test_sources.py` — new test class (see Tests).

## Steps

1. Apply the one-line param fix + comment in `fetch_ohlc_page`.
2. Add the two tests below (stub the `self._http` context manager — an object
   with `__aenter__`/`__aexit__` returning a recorder whose
   `get(url, params)` captures `params` and returns canned OKX
   `{"code":"0","data":[...]}` payloads).
3. Run `pytest` (full suite) and `ruff check dccd/`.

## Tests

`dccd/tests/v3/test_sources.py`, new class `TestOKXOHLCWindowBoundary`:

- `test_before_param_is_exclusive_adjusted` — call
  `fetch_ohlc_page(Symbol("BTC","USDT"), 60, start_ns, end_ns, 100)` against the
  recording stub; assert `params["before"] == str(start_ns // 1_000_000 - 1)`
  and `params["after"] == str(end_ns // 1_000_000)`.
- `test_no_bar_lost_at_page_boundary` — the regression guard. Fake `get`
  emulates OKX exclusive semantics: from a synthetic continuous 1m series,
  return (newest-first, max 100) the bars with
  `before_ms < ts_ms < after_ms` — both bounds **strictly** exclusive. Drive
  `paginate_ohlc` (via a closure over the adapter, per the paginator contract)
  across a window of ≥ 150 minutes so at least one 100-bar page boundary is
  crossed; assert the collected timestamps are exactly every minute of the
  requested `[start, end]` window — no hole at the boundary, no duplicate.

## Verification on real data

`data-e2e` discipline, isolated store (`/tmp`), live OKX REST:

- Run a real `backfill` of `okx BTC/USDT ohlc 1m` over a fixed ≥ 12 h window
  (≥ 7 page boundaries).
- Read the Parquet back: assert `rows == expected_rows` for the window
  (i.e. inventory gap detection reports `missing_rows == 0`) and that the
  previously-lost boundary minutes (`start + k·100min`) are present.

## Closeout

- CHANGELOG `Fixed`: "OKX OHLC pagination silently dropped the bar at every
  100-bar page boundary — OKX `before`/`after` cursors are exclusive; the
  window start is now passed as `start − 1 ms` so it is inclusive (#NN)"
- ADR: none — mechanical off-by-one; the exclusivity note lives in the code
  comment and the regression test.
- Status/roadmap: remove the OKX bullet from `## Audit fixes (2026-06-12)` in
  `doc/dev/07-roadmap.md`; note in `06-status.md` that arthurserver's OKX gaps
  need the post-deploy re-backfill (ops step tracked in the epic's
  `00-plan.md` Done criteria).
