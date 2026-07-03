---
plan: derivative-markets/03-funding-datatype
kind: leaf
status: planned
complexity: high
model: sonnet
depends: [01, 02]
parallel: false
branch: feat/funding-datatype
pr: ""
---

# DataType.FUNDING end-to-end, Binance first

## Goal

`DataType.FUNDING` exists across every layer (domain record → Parquet schema/
path/dedup → source protocol → backfill branch → config/CLI/API pass-through)
and `backfill binance "BTC/USDT:perp" funding start=2019-09-01` lands the
full realized-funding history on disk. Scan row 1. First-of-its-kind leaf —
the framework the next three leaves ride on.

## Files to change

- `dccd/domain/types.py` — `FUNDING = "funding"` (update the docstring).
- `dccd/domain/records.py` — `FundingRate(BaseModel, frozen=True)`:
  `ts: int` (ns UTC, the funding time), `rate: float`,
  `mark_price: float | None = None`. numpydoc docstring like siblings.
- `dccd/storage/parquet.py` —
  - `_FUNDING_SCHEMA = {"TS": pl.Int64, "rate": pl.Float64,
    "mark_price": pl.Float64}`; register in `_SCHEMAS`.
  - `_to_dataframe`: FUNDING branch.
  - `_dedup_subset`: FUNDING → `["TS"]` (one funding event per instant).
  - `directory()`: FUNDING is flat — `root / "funding" / pair_slug` (no span
    subdir; funding interval varies per symbol and is not a job parameter).
  - `_period_fmt`: annual (`"%Y"`) for OHLC **and** FUNDING (≈1 095 rows/yr
    at 8h; daily files would be pathological).
  - `inventory()`: accept dtype dir `"funding"` (flat like trades — but
    **annual** files; rows/min/max/bytes as usual, `expected_rows`/
    `missing_rows` stay `None`: no fixed interval to do the arithmetic with).
- `dccd/sources/base.py` — `FundingHistory(Source)` protocol:
  `async def fetch_funding_page(self, symbol, start_ns, end_ns, limit,
  cursor=None) -> tuple[list[FundingRate], str | None]` — same opaque-cursor
  contract as `TradesHistory` (copy that class-docstring contract, adapted).
- `dccd/application/operations.py` —
  - `_DEFAULT_LOOKBACK_NS[DataType.FUNDING] = 365 * 86400 * NS` (a year of
    funding ≈ 1 095 records — cheap; extend the "No existing data" human
    label accordingly).
  - FUNDING branch in `backfill()`, mirroring the TRADES branch:
    `isinstance(adapter, FundingHistory)` + capability lookup +
    `_check_market(cap, target)`, then drive the fetch closure with
    `paginate_trades` (it is duck-typed on `.ts` — add a comment where it's
    imported that it drives any cursor-paged record stream, and rename
    nothing).
- `dccd/sources/binance.py` — implement `FundingHistory`:
  - `fetch_funding_page`: GET `{_BASE_FAPI}/fundingRate` with
    `{"symbol": render_symbol, "startTime": int(cursor) if cursor else
    start_ns // 1_000_000, "endTime": end_ns // 1_000_000, "limit":
    min(limit, 1000)}`. Response is an **ascending** JSON list of
    `{"fundingTime": ms, "fundingRate": str, "markPrice": str-or-empty}` →
    `FundingRate(ts=fundingTime * 1_000_000, rate=float(fundingRate),
    mark_price=float(markPrice) if markPrice else None)`.
    `next_cursor = str(last fundingTime + 1)` when `len(data) == limit`,
    else `None`.
  - `capabilities()`: `Capability(data_type=FUNDING, transport="rest",
    mode="historical", history="full", max_per_request=1000,
    page_direction="forward", markets=["perp"])`.
- `dccd/tests/v3/` — see Tests.

Check-only (change if needed): `interfaces/cli/main.py` passes
`DataType(data_type)` generically — `-t funding` should just work;
`interfaces/api/app.py` span-required check at ~L581 only concerns `ohlc` —
funding must pass with `span=None`.

## Steps

1. Domain: enum + record (+ exports in `__all__`).
2. Storage: schema, `_to_dataframe`, dedup, directory, period, inventory.
3. Protocol in `sources/base.py` (+ `__all__`).
4. Operation branch + default lookback.
5. Binance adapter method + capability.
6. `pytest` + `ruff check dccd/`.

## Tests

- `test_domain.py` (or `_extended`) — `DataType("funding")`; `FundingRate`
  construction/frozen.
- `test_storage.py` — save/load round-trip of `FundingRate` records: path is
  `funding/BTC-USDT_PERP/2026.parquet` (annual), dedup on TS (saving the
  same event twice keeps one row), `canonicalize` idempotent on the funding
  schema, `inventory()` lists the dataset with `data_type="funding"`.
- `test_application.py` — backfill FUNDING with a fake cursor-paged adapter:
  drains multiple pages, respects `[start, end]`, market check rejects a
  spot-market target (Binance declares `markets=["perp"]`).
- `test_sources.py` — Binance funding param/cursor construction (stubbed
  client): first call has `startTime=start_ms`, follow-up uses the cursor,
  short page → `next_cursor is None`; capability declared with
  `markets=["perp"]`.

## Verification on real data

Isolated store, real Binance fapi:

1. `backfill binance "BTC/USDT:perp" funding start=2019-09-01` — full
   history. Assert: first TS ≈ 2019-09-10 (BTCUSDT perp funding inception);
   count is plausible for ~8h cadence (order 7 000–8 000 by mid-2026); TS
   strictly increasing after load (store sorts), **zero duplicate TS**;
   all |rate| ≤ 0.0075 (Binance clamp).
2. Spot-check the 5 earliest and 5 latest rows against a direct `curl` of
   `/fapi/v1/fundingRate` (values must match exactly).
3. Re-run with `start=last` — idempotent: ≤1 new row, row count stable
   (dedup proof).
4. `dccd read`/`inventory` surface the dataset (CLI path works end-to-end).

## Closeout

- CHANGELOG (`Added`): "DataType.FUNDING — perp funding-rate history
  (domain record, Parquet store `funding/{pair}/YYYY.parquet`, cursor
  backfill); Binance USDS-M as first source, full history (#NN)"
- ADR: one entry — funding storage/pagination choices: flat annual layout
  (variable per-symbol interval ⇒ no span dir, no gap arithmetic), cursor
  contract shared with trades (fixed windows would drop overflow on 1h-4h
  funding symbols), 365-day bounded default lookback.
- Status/roadmap: deferred to leaf 07.
