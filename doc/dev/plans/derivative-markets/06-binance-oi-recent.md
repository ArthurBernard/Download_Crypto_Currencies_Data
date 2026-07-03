---
plan: derivative-markets/06-binance-oi-recent
kind: leaf
status: planned
complexity: medium
model: sonnet
depends: [05]
parallel: false
branch: feat/binance-oi-recent
pr: ""
---

# Binance OI forward collector (30-day hard cap, declared honestly)

## Goal

Binance open-interest statistics are collectable on a schedule despite the
documented **hard 30-day history cap** (scan row 4): the capability declares
`history="recent"` + `recent_window_s=30d`, backfill clamps + warns (leaf 05
machinery), and recurring jobs can accumulate history forward.
**Time-sensitive**: every week before the collector runs in prod is data
lost forever — this leaf should merge and deploy promptly.

## Files to change

- `dccd/sources/binance.py` — implement `OpenInterestHistory`:
  - constant `_BASE_FDATA = "https://fapi.binance.com/futures/data"`.
  - `fetch_oi_page`: GET `{_BASE_FDATA}/openInterestHist` with
    `{"symbol": render_symbol, "period": binance_interval(span), "limit":
    min(limit, 500), "startTime": int(cursor) if cursor else
    start_ns // 1_000_000, "endTime": end_ns // 1_000_000}`.
    `period` accepts exactly {5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d} —
    `binance_interval` already emits these strings for the matching spans.
    Response ascending:
    `{"sumOpenInterest": str, "sumOpenInterestValue": str, "timestamp": ms}`
    → `OpenInterest(ts=timestamp * 1_000_000,
    open_interest=float(sumOpenInterest),
    open_interest_value=float(sumOpenInterestValue))`.
    `next_cursor = str(last_ts_ms + span * 1000)` when
    `len(data) == limit`, else `None` (forward walk, same style as leaf 03
    Binance funding).
  - `capabilities()`: `Capability(data_type=OPEN_INTEREST, transport="rest",
    mode="historical", history="recent", recent_window_s=30 * 86400,
    max_per_request=500, page_direction="forward", markets=["perp"],
    spans=[300, 900, 1800, 3600, 7200, 14400, 21600, 43200, 86400])`.
- `dccd/tests/v3/test_sources.py` — see Tests.

## Steps

1. Adapter method + capability (the clamp logic already exists from 05 —
   nothing to add in operations).
2. `pytest` + `ruff check dccd/`.

## Tests

- `test_sources.py` — param construction (period string per span; cursor
  walk forward; short page ends); capability declares
  `history="recent"` + `recent_window_s=30*86400` + `markets=["perp"]`.
- `test_application.py` — nothing new (clamp covered in 05 with a fake);
  optionally one integration-style test asserting the Binance OI capability
  triggers the clamp path.

## Verification on real data

Isolated store, real Binance fapi:

1. `backfill binance "BTC/USDT:perp" open_interest span=3600 start=origin`
   — must **clamp with the warning log** and land ≈ 720 hourly rows
   (30 days), earliest TS ≈ now − 30d. The clamp firing at the declared
   boundary IS the honesty proof.
2. Spot-check 5 values against a direct `curl` of `openInterestHist`.
3. 5m run over 24h — ~288 rows; re-run `start=last` — idempotent (this is
   the exact recurring-job pattern prod will use).

## Closeout

- CHANGELOG (`Added`): "Binance USDS-M open-interest statistics —
  forward collector with honestly-declared 30-day window
  (`history='recent'`, clamp + warn) (#NN)"
- ADR: none — mechanical application of the 05 design.
- Status/roadmap: deferred to leaf 07 — **but flag in the PR description**:
  after the epic's `/release`, immediately add recurring OI jobs on
  arthurserver (at least BTC/ETH/SOL USDT perps, 1h + 5m, Bybit and
  Binance) — the 30-day cap makes delay irreversible.
