---
plan: audit-fixes/07-gap-metric-context
kind: leaf
status: done
complexity: medium
depends: []
parallel: false
branch: feat/gap-metric-context
pr: ""
---

# UX — gap % must not call trade-less minutes "missing"

## Goal

`inventory()`'s OHLC gap detection (`expected_rows`/`missing_rows`,
`storage/parquet.py` ~lines 430–465) divides elapsed time by the span:
illiquid pairs on exchanges that emit no empty candles (bitfinex BTC-EUR 1m)
show "85 % missing" when nothing is missing. Make the metric honest without
breaking its real purpose (catching collection holes on liquid pairs —
binance 1m showed 0 gaps over 44k bars).

## Files to change

- `dccd/storage/parquet.py` — keep the zero-extra-I/O constraint (footer
  stats only). Cheap, honest option: also expose the **largest single gap**
  (`max_gap_rows` derivable? footer stats give min/max/rowcount only — so
  no; then the fix is presentational). Do NOT add per-file scans.
- `dccd/interfaces/ui/templates/data.html` — rename the column/label from
  "missing"/"gap %" to **"candle coverage"** with a tooltip: "Exchanges emit
  no candle for minutes without trades — low coverage on quiet pairs is
  normal, not data loss." Threshold styling (red dot) should key on
  *exchange-relative* expectations if cheap, else drop the alarming color
  below a documented floor.
- `doc/source/` page documenting the inventory fields — same wording.

## Steps

1. Confirm with the footer-stats cache exactly what is computable for free;
   if nothing structural is free, this is a presentation + naming fix (the
   audit explicitly allows "or at least label it in the Data UI").
2. Apply the rename + tooltip + de-alarming in `data.html` (and any API
   field names left as-is for compatibility — UI label only).
3. Sphinx page wording; `cd doc && make html` → 0 warnings.
4. `pytest`, `ruff`; UI smoke (`doc/dev/ui_smoke.py`) against an isolated
   `dccd ui`.

## Tests

- Existing storage tests stay green (no semantic change to the numbers).
- If any computed field changes, `test_storage*.py` asserts the new shape.

## Verification on real data

- Isolated store: backfill a liquid pair (binance BTC/USDT 1m, a few days)
  AND an illiquid one (bitfinex or kraken minor pair, 1m): the Data page
  shows full coverage for the first and *non-alarming* coverage wording for
  the second; a deliberately holed dataset (delete a middle file) still
  surfaces visibly.

## Closeout

- CHANGELOG (`Changed`): "Data page presents OHLC completeness as 'candle
  coverage' and explains trade-less minutes, instead of mislabeling quiet
  pairs as missing data (#NN)"
- ADR: short note — why presentational (footer-stats-only constraint).
- Status/roadmap: tick leaf in 00-plan.
