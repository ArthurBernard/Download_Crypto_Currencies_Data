---
plan: derivative-markets/02-binance-futures-klines
kind: leaf
status: planned
complexity: medium
model: sonnet
depends: [01]
parallel: false
branch: feat/binance-futures-klines
pr: ""
---

# Binance perp + quarterly futures klines (the basis leg)

## Goal

`BinanceSource.fetch_ohlc_page` serves non-spot symbols from the USDS-M
futures API (`continuousKlines`), so `backfill binance "BTC/USDT:quarter"
ohlc` collects the auto-rolled quarterly series (basis = quarter close − spot
close, computed later in fynance-research) and `"BTC/USDT:perp"` collects
perp OHLC. Scan row 10 — not a new DataType; OHLC machinery reused verbatim.

## Files to change

- `dccd/sources/binance.py` —
  - constants: `_BASE_FAPI = "https://fapi.binance.com/fapi/v1"` and
    `_CONTRACT_TYPE = {"perp": "PERPETUAL", "quarter": "CURRENT_QUARTER",
    "next_quarter": "NEXT_QUARTER"}`.
  - `fetch_ohlc_page`: when `symbol.market != "spot"`, GET
    `{_BASE_FAPI}/continuousKlines` with params
    `{"pair": self.render_symbol(symbol), "contractType":
    _CONTRACT_TYPE[symbol.market], "interval": interval, "startTime":
    start_ns // 1_000_000, "endTime": end_ns // 1_000_000, "limit":
    min(limit, 1500)}`. Response rows have the same 12-field kline layout as
    spot — reuse `_parse_ohlc_page` unchanged. Spot path untouched.
  - `capabilities()`: OHLC rest/historical capability gains
    `markets=["spot", "perp", "quarter", "next_quarter"]`. WS caps stay
    `markets=None` (no live futures streams in this epic — keep honest).
- `dccd/tests/v3/test_sources.py` — routing + capability tests.

## Steps

1. Add constants + the market branch in `fetch_ohlc_page` (keep
   `max_per_request=1000` on the shared capability; fapi's 1500 is not worth
   a second capability).
2. Declare `markets` on the OHLC REST capability.
3. Rate limiting: keep the shared `"binance"` limiter bucket (conservative —
   fapi actually has separate limits; note this in a code comment).
4. `pytest` + `ruff check dccd/`.

## Tests

- `test_sources.py` — with a stubbed HTTP client capturing `(url, params)`:
  spot symbol → `/api/v3/klines` with `symbol=`; perp → `/fapi/v1/continuousKlines`
  with `pair=BTCUSDT&contractType=PERPETUAL`; quarter → `CURRENT_QUARTER`;
  `next_quarter` → `NEXT_QUARTER`; limit clamped to ≤1500. Capability test:
  OHLC REST declares the four markets; WS caps declare none.

## Verification on real data

Isolated store (temp `data_path`), real Binance API:

1. `backfill binance "BTC/USDT:quarter" ohlc span=86400 start=2023-01-01` —
   read the Parquet back: rows ≈ days elapsed since 2023-01-01, **no gap and
   no duplicate TS across quarterly roll dates** (late Mar/Jun/Sep/Dec — the
   whole point of the continuous series), path
   `binance/ohlc/BTC-USDT_QUARTER/1d/*.parquet`.
2. Cross-check 3 sampled daily closes against a direct `curl` of
   `continuousKlines`.
3. `backfill binance "BTC/USDT:perp" ohlc span=3600 start=<7 days ago>` —
   ~168 bars, sane prices (within a few % of spot).
4. Sanity: quarter close vs spot close basis magnitude < ~5 % annualised on
   the sampled dates (catches a wrong-contract bug immediately).

## Closeout

- CHANGELOG (`Added`): "Binance USDS-M continuous-contract klines: perp and
  quarterly futures OHLC via `BTC/USDT:perp` / `:quarter` /
  `:next_quarter` symbols (basis leg for research) (#NN)"
- ADR: none — mechanical application of the 01 design (the design rationale
  lives in the 01 entry + 00-plan).
- Status/roadmap: deferred to leaf 07.
