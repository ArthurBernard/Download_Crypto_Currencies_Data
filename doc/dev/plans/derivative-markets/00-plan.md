---
plan: derivative-markets
kind: global
status: planning
roadmap: "- [ ] **Derivative markets** — `DataType` for funding / open-interest / liquidations, `Symbol.market=perp`. **Sequencing informed by the 2026-07 data scan**"
release_on_done: true
---

# Derivative markets — funding, open interest, futures klines (basis)

## Goal

dccd collects perpetual-futures **funding rates** (Binance + Bybit, full
history), **open interest** (Bybit deep history + Binance 30-day forward
collector), and **quarterly-futures klines** (Binance continuous contracts —
the futures leg of the basis signal), all through the existing hexagonal
pipeline: honest `Capability` declarations, cursor pagination, Parquet storage
with per-type dedup keys, jobs schedulable from config/API/UI. Sequencing and
endpoint facts come from the verified 2026-07 scan
([`../data-sources-scan-2026-07.md`](../data-sources-scan-2026-07.md), rows
1, 2, 4, 5, 10).

**Execution model note**: per Arthur's decision (2026-07-03), every leaf is
executed by a **sonnet** agent (`model: sonnet` in each leaf's frontmatter —
explicit user override of the global "opus always" rule for this epic).
`complexity` stays honest for effort/ordering.

## Design decisions (fixed here so leaves never re-decide)

1. **`Symbol.market`** becomes `Literal["spot", "perp", "quarter",
   "next_quarter"]` (default `"spot"`). String form: `"BTC/USDT"` for spot
   (unchanged), `"BTC/USDT:perp"` otherwise; `Symbol.parse` accepts the
   suffix. `DatasetId.pair_slug()` appends `_PERP` / `_QUARTER` /
   `_NEXT_QUARTER` for non-spot, so on-disk trees stay separate
   (`funding/BTC-USDT_PERP/…`). `JobSpec.make_id` picks the market up for
   free via `str(symbol)` — spot and perp jobs can't collide.
2. **Capability honesty extended**: `Capability.markets: list[str] | None`
   (`None` = spot-only, keeps all existing declarations honest) and
   `Capability.recent_window_s: int | None` (declared recent-history window;
   Binance OI = 30 days). `backfill()` rejects an undeclared market with
   `NoCapability` and clamps + warns on `recent_window_s`, mirroring the
   Kraken 720-bar clamp.
3. **New DataTypes**: `FUNDING = "funding"` (record `FundingRate(ts, rate,
   mark_price?)`, flat dir `funding/{pair}/YYYY.parquet`, dedup `TS`) and
   `OPEN_INTEREST = "open_interest"` (record `OpenInterest(ts, open_interest,
   open_interest_value?)`, span-typed dir
   `open_interest/{pair}/{span}/YYYY.parquet`, dedup `TS`, span required like
   OHLC). Both annual files (low row rates), both REST-historical only — no
   live/WS variants in this epic.
4. **Pagination**: funding and OI use the **cursor contract** of
   `TradesHistory` (opaque adapter cursor draining `[start, end]`), driven by
   the existing `paginate_trades` loop (duck-typed on `.ts`). Fixed-window
   pagination is unsafe here (funding intervals vary 1h–8h per symbol; a full
   window would silently drop overflow).
5. **Quarterly klines are NOT a new DataType** — `DataType.OHLC` against
   `fapi/v1/continuousKlines` (`contractType` mapped from `Symbol.market`),
   reusing `OHLCBar`, `canonicalize()` and the OHLC storage layout verbatim.
6. **Descoped / re-homed** (record in the ADR at closeout):
   - **Liquidations** — descoped from the epic (scan row 8: WS-only,
     forward-only, exchange-side lossy sampling; architecturally unlike the
     rest).
   - **Long/short + taker ratios** (scan row 7) — deferred to the
     *metric-series* epic where their `(ts, metric, value)` shape belongs;
     they share Binance's 30-day cap so they stay time-sensitive there.
   - **OKX funding/OI** (rows 3, 6) — cheap follow-ons once this framework
     exists, but depth unverified; new roadmap line, not leaves here.
   - **Dedicated Binance basis endpoint** (row 16) — redundant with quarterly
     klines for research use.

## Decomposition

1. **symbol-market** — `Symbol.market` + `Capability.markets`/
   `recent_window_s` + market check in `backfill()`: the domain plumbing
   everything else stands on.
2. **binance-futures-klines** — route `fetch_ohlc_page` by market to
   `continuousKlines` (perp + quarterly OHLC → basis leg). Cheapest win.
3. **funding-datatype** — `DataType.FUNDING` end-to-end (domain → storage →
   protocol → operation) with Binance as first adapter, full history.
4. **bybit-funding** — second funding adapter (paired-params backward walk,
   empirical depth probe before declaring `history`).
5. **open-interest-datatype** — `DataType.OPEN_INTEREST` end-to-end with
   Bybit first (deep history, backtestable).
6. **binance-oi-recent** — Binance OI forward collector (30-day cap declared
   via `recent_window_s`; **time-sensitive** — every week of delay is data
   lost forever).
7. **derivatives-ui-docs** — Historical/Data UI tabs, `how-to/derivatives`,
   capability matrix, CLAUDE.md, roadmap/ADR closeout.

## Leaf checklist

- [x] 01 symbol-market — feat/symbol-market — medium
- [ ] 02 binance-futures-klines — feat/binance-futures-klines — medium (depends on 01)
- [ ] 03 funding-datatype — feat/funding-datatype — high (depends on 01, 02)
- [ ] 04 bybit-funding — feat/bybit-funding — medium (depends on 03)
- [ ] 05 open-interest-datatype — feat/open-interest-datatype — high (depends on 03, 04)
- [ ] 06 binance-oi-recent — feat/binance-oi-recent — medium (depends on 05)
- [ ] 07 derivatives-ui-docs — feat/derivatives-ui-docs — medium (depends on 06)

## Dependencies

- 02 depends on 01 (market plumbing).
- 03 depends on 01, 02 (perp symbols; serialises `binance.py` edits).
- 04 depends on 03 (funding framework).
- 05 depends on 03, 04 (storage/operations patterns; serialises `bybit.py`).
- 06 depends on 05 (OI framework + `recent_window_s` clamp).
- 07 depends on 06 (surfaces everything).
- No `parallel: true` anywhere — the chain shares files at every step.

## Done criteria

- Full unit suite + `ruff check dccd/` green at every leaf; Sphinx builds
  with 0 warnings at leaf 07.
- Real-data verification recorded per leaf (isolated store, `data-e2e`
  discipline): funding full-history Binance + Bybit; quarterly klines deep
  and continuous across roll dates; Bybit OI deep; Binance OI clamped +
  warned exactly at its declared window.
- Every new capability declaration is honest (markets, history depth,
  spans, page sizes verified against the live API during leaf execution).
- Roadmap item "Derivative markets" removed at leaf 07; descoped items
  re-homed (liquidations tombstoned in the ADR, ratios noted in the
  metric-series roadmap line, OKX follow-on line added).
- **Post-release ops note**: start recurring Binance OI (+ Bybit OI) jobs on
  arthurserver immediately after `/release` — the 30-day cap makes delay
  irreversible.
