---
plan: kraken-futures/02-charts-klines-docs
kind: leaf
status: planned
complexity: medium
depends: [01]
parallel: false
branch: feat/kraken-futures-klines
pr: ""
---

# Kraken Futures perp klines (charts API) + docs closeout

## Goal

`backfill krakenfutures "BTC/USD:perp" ohlc span=3600` collects deep perp
OHLC via the charts API, and the epic closes out (capability matrix,
CLAUDE.md, how-to cadence caveat, roadmap line removed).

## Files to change

- `dccd/domain/timeutils.py` — `kraken_futures_resolution(span) -> str | None`:
  `{60: "1m", 300: "5m", 900: "15m", 1800: "30m", 3600: "1h", 14400: "4h",
  43200: "12h", 86400: "1d", 604800: "1w"}`.
- `dccd/sources/kraken_futures.py` — add `OHLCHistory`:
  - `_BASE_CHARTS = "https://futures.kraken.com/api/charts/v1"`.
  - `fetch_ohlc_page(symbol, span, start_ns, end_ns, limit)`:
    GET `{_BASE_CHARTS}/trade/{render_symbol(symbol)}/{resolution}` with
    `{"from": start_ns // NS, "to": end_ns // NS}` (**seconds**, not ms —
    live-probed). Unsupported span → `[]`. Response
    `{"candles": [{"time": ms, "open": str, "high": str, "low": str,
    "close": str, "volume": str}, ...]}` ascending, anchored on `from`,
    ~2 000 max → `OHLCBar(ts=time * 1_000_000, ... float(str fields),
    quote_volume=None, trades=None)`.
  - `capabilities()`: add `Capability(data_type=DataType.OHLC,
    transport="rest", mode="historical", history="full",
    max_per_request=2000, page_direction="forward", markets=["perp"],
    spans=[60, 300, 900, 1800, 3600, 14400, 43200, 86400, 604800])`.
- `doc/dev/04-exchanges.md` — Derivative-data matrix: `krakenfutures` row
  (funding hourly/1-yr window; klines deep via charts API; OI snapshot-only
  = not implemented, by design).
- `CLAUDE.md` — adapter table: one `kraken_futures.py` row (funding 1h
  recent-1y + perp klines; separate API surface from spot Kraken).
- `doc/source/how-to/derivatives.rst` — add Kraken Futures to the funding
  section with the **cadence caveat** (1h vs 8h — normalise before
  cross-exchange comparison) and the 1-year-window note; klines example.
- `doc/dev/07-roadmap.md` — handled at closeout by the orchestrator (this is
  the last leaf: the "Kraken Futures adapter" line is removed).
- `dccd/tests/v3/test_sources.py`, `test_domain.py` — see Tests.

## Steps

1. Resolution mapper + adapter OHLC method + capability.
2. Docs: 04-exchanges, CLAUDE.md, how-to.
3. `pytest` + `ruff check dccd/` + `cd doc && make html` (0 warnings).

## Tests

- `test_sources.py` — charts URL construction (`trade/PF_XBTUSD/1h`,
  from/to in seconds); string-field parsing to floats; unsupported span
  (e.g. 7200) → `[]` with no HTTP call; capability (spans list,
  `max_per_request=2000`, `markets=["perp"]`, `history="full"`); WS caps
  still absent.
- `test_domain.py` — `kraken_futures_resolution` mapping + `None` for
  unsupported spans.

## Verification on real data

Isolated store, real charts API:

1. `backfill krakenfutures "BTC/USD:perp" ohlc span=86400 start=2022-04-01`
   — rows ≈ days elapsed, 0 dup TS, `missing_rows` ≈ 0, earliest TS ≈ the
   requested start; path `krakenfutures/ohlc/BTC-USD_PERP/1d/*.parquet`.
2. `span=3600` over the last 14 days — ≈ 336 rows, gap-free (proves the
   2 000-candle forward paging across at least one page boundary at finer
   spans: also run `span=300` over 10 days ≈ 2 880 rows → ≥ 2 pages, assert
   no gap and no dup at the page seam).
3. Cross-check 3 sampled daily closes against a direct `curl` (exact match);
   sanity: closes within a few % of Binance perp closes on the same days.
4. Re-run `start=last` — idempotent.

## Closeout

- CHANGELOG (`Added`): "Kraken Futures perp klines via the charts API
  (deep history, 2000-candle pages, 9 resolutions) (#NN)"
- ADR: none expected — mechanical application of the epic design (rationale
  in the leaf-01 entry); add one only if a real endpoint surprise forces a
  design change (document it then).
- Status/roadmap: THIS leaf's closeout (orchestrator): status entry for the
  epic, remove the roadmap line, archive the tree, tick the global; note the
  ops follow-up (funding jobs on arthurserver within the quarter).
