---
plan: derivative-markets/05-open-interest-datatype
kind: leaf
status: planned
complexity: high
model: sonnet
depends: [03, 04]
parallel: false
branch: feat/open-interest-datatype
pr: ""
---

# DataType.OPEN_INTEREST end-to-end, Bybit first (deep history)

## Goal

`DataType.OPEN_INTEREST` exists across every layer, span-typed like OHLC,
and `backfill bybit "BTC/USDT:perp" open_interest span=3600
start=2022-01-01` lands a deep, backtestable OI series. Bybit first (scan
row 5: history back to symbol launch — no urgency cap), Binance in leaf 06.

## Files to change

- `dccd/domain/types.py` — `OPEN_INTEREST = "open_interest"`.
- `dccd/domain/records.py` — `OpenInterest(BaseModel, frozen=True)`:
  `ts: int` (ns UTC), `open_interest: float` (contracts/base units),
  `open_interest_value: float | None = None` (notional, when provided).
- `dccd/domain/timeutils.py` — `bybit_oi_interval(span) -> str | None`:
  `{300: "5min", 900: "15min", 1800: "30min", 3600: "1h", 14400: "4h",
  86400: "1d"}` (Bybit OI uses these strings, NOT the kline codes).
- `dccd/storage/parquet.py` —
  - `_OI_SCHEMA = {"TS": pl.Int64, "open_interest": pl.Float64,
    "open_interest_value": pl.Float64}`; register in `_SCHEMAS`.
  - `_to_dataframe` branch; `_dedup_subset` → `["TS"]`.
  - `directory()`: span subdir **like OHLC** —
    `root / "open_interest" / pair_slug / span_label(span)`; raise
    `ValueError` when `span is None` (mirror the OHLC message).
  - `_period_fmt`: annual (`"%Y"`) — 5m OI is ~105k rows/yr, fine annual.
  - `inventory()`: handle `"open_interest"` exactly like `"ohlc"` (span
    dirs, and the same `expected_rows`/`missing_rows` arithmetic — OI has a
    fixed interval so the gap math is valid).
- `dccd/sources/base.py` — `OpenInterestHistory(Source)`:
  `async def fetch_oi_page(self, symbol, span, start_ns, end_ns, limit,
  cursor=None) -> tuple[list[OpenInterest], str | None]` (same cursor
  contract; `span` bound in the closure like OHLC's).
- `dccd/application/operations.py` —
  - `_DEFAULT_LOOKBACK_NS[DataType.OPEN_INTEREST] = 30 * 86400 * NS`.
  - OPEN_INTEREST branch in `backfill()`: `isinstance(adapter,
    OpenInterestHistory)` + cap lookup + `_check_market`; validate
    `target.span` against `cap.spans` (same error text pattern as OHLC);
    **recent-window clamp**: if `cap.history == "recent"` and
    `cap.recent_window_s`, clamp `start_ns` to
    `end_ns - cap.recent_window_s * NS` with the warning log (mirror the
    Kraken clamp wording; used by Binance in leaf 06); drive with the
    cursor paginator (closure binds `symbol` and `span`).
- `dccd/application/config.py` — span required for OHLC **and**
  OPEN_INTEREST: extend `_validate_span_for_ohlc` (rename to
  `_validate_span_required`) to `data_type in ("ohlc", "open_interest")`.
- `dccd/interfaces/api/app.py` (~L581) — same extension of the
  span-required check for job creation.
- `dccd/sources/bybit.py` — implement `OpenInterestHistory`:
  - `fetch_oi_page`: GET `{_BASE}/open-interest` with
    `{"category": "linear", "symbol": ..., "intervalTime":
    bybit_oi_interval(span), "startTime": start_ns // 1_000_000,
    "endTime": end_ns // 1_000_000, "limit": min(limit, 200)}` plus
    `"cursor": cursor` when set — Bybit provides a **real**
    `result.nextPageCursor` here; pass it through as the opaque cursor
    (empty string → `None`). `result.list` is newest-first:
    `{"openInterest": str, "timestamp": str-ms}` →
    `OpenInterest(ts=int(timestamp) * 1_000_000,
    open_interest=float(openInterest))` (no notional on this endpoint).
    `retCode` check as usual.
  - `capabilities()`: `Capability(data_type=OPEN_INTEREST, transport="rest",
    mode="historical", history="full", max_per_request=200,
    page_direction="backward", markets=["perp"],
    spans=[300, 900, 1800, 3600, 14400, 86400])`.
- `dccd/tests/v3/` — see Tests.

## Steps

1. Domain: enum, record, `bybit_oi_interval`.
2. Storage: schema/branches/directory/period/inventory.
3. Protocol; operations branch (span check + recent clamp + lookback).
4. Config + API span-required extension.
5. Bybit adapter + capability.
6. `pytest` + `ruff check dccd/`.

## Tests

- `test_storage.py` — round-trip: path
  `open_interest/BTC-USDT_PERP/1h/2026.parquet`; dedup TS; `inventory()`
  reports span + `expected_rows`/`missing_rows` for OI.
- `test_domain.py` — `bybit_oi_interval` mapping incl. unsupported span →
  `None`; `JobConfig(data_type="open_interest")` without span raises; with
  span validates.
- `test_application.py` — OI branch: span-not-in-caps rejected; recent
  clamp fires exactly at `end - recent_window_s` with a warning (fake
  adapter with `history="recent"`, `recent_window_s=30*86400`); market
  check enforced.
- `test_sources.py` — Bybit OI params (`category`, `intervalTime`, both
  time bounds, limit ≤200), `nextPageCursor` passthrough, newest-first
  parsing.
- `test_api.py` — job create `open_interest` without span → 400; with span
  → 200.

## Verification on real data

Isolated store, real Bybit API:

1. `backfill bybit "BTC/USDT:perp" open_interest span=3600
   start=2022-01-01` — rows ≈ hours elapsed (±small exchange gaps), zero
   dup TS, `inventory()` gap % near 0, earliest TS ≈ the requested start
   (proving the deep-history claim the capability declares).
2. Spot-check 5 values against a direct `curl` (exact match).
3. 5m sanity run over the last 48h — ~576 rows.
4. Re-run `start=last` — idempotent.

## Closeout

- CHANGELOG (`Added`): "DataType.OPEN_INTEREST — span-typed OI history
  (`open_interest/{pair}/{span}/YYYY.parquet`, gap detection); Bybit first
  with full history to symbol launch (#NN)"
- ADR: one entry — OI is span-typed like OHLC (fixed interval ⇒ span dirs +
  gap arithmetic for free), `recent_window_s` clamp generalises the Kraken
  720-bar honesty pattern to time-bound windows.
- Status/roadmap: deferred to leaf 07.
