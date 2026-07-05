# Data-acquisition scan for the fynance-research stack

Scope: survey what's realistic to add to **dccd** (multi-exchange OHLCV collector,
`/home/arthur/dev/Download_Crypto_Currencies_Data`) beyond 1m spot OHLCV — funding,
open interest, liquidations, options vol, basis, on-chain, sentiment — for
fynance-research to consume. Written incrementally as sources are verified.

---

## PART 1 — dccd today (effort baseline)

**Architecture**: hexagonal. `domain/` (pure) -> `transport/` (async I/O) ->
`sources/` (7 exchange adapters: binance, bybit, coinbase, kraken, okx, bitfinex,
bitmex) -> `storage/` (Parquet + SQLite run history) -> `application/` (operations,
scheduler, config) -> `interfaces/` (CLI/API/UI/Client).

**The load-bearing fact for effort estimation**: `domain/types.py` defines a
**closed** `DataType` enum — currently only `OHLC | TRADES | ORDERBOOK`. Every
other layer keys off it:
- `domain/records.py` — one Pydantic record model per data type (`OHLCBar`,
  `Trade`, `OrderBookSnapshot`).
- `storage/parquet.py` — `canonicalize(df, data_type)` picks a fixed schema per
  type, `_dedup_subset` picks the dedup key per type (OHLC=`TS`, trades=`tid`,
  book=`(TS,side,price)`), and the on-disk path pattern is type-specific
  (`ohlc/{pair}/{span}/YYYY.parquet` annual vs `trades|orderbook/{pair}/YYYY-MM-DD.parquet`
  daily).
- `sources/base.py` — protocol mixins per data type x transport
  (`OHLCHistory`, `TradesHistory`, `OrderBookSnapshotREST`, `*Live`); adapters
  declare `Capability(data_type, transport, mode, history=full|recent,
  max_per_request, spans, max_depth, ...)` and the engine's `NoCapability`
  check enforces honesty — nothing runs unless declared.

**Effort ladder this implies:**
- **New REST endpoint on an existing exchange, existing DataType** (e.g. a
  second Binance OHLC interval, or backfilling a missing symbol/date range) —
  **S**. Touches one adapter file only; storage/domain untouched.
- **New provider/exchange, existing DataTypes** (e.g. an 8th spot exchange) —
  **S/M**. One new `sources/<x>.py` implementing the base protocols +
  registration in `service_factory.build_registry()`; storage/domain
  untouched. `04-exchanges.md`'s per-exchange caveats (pagination direction,
  symbol format, rate limits) are the real variable cost.
- **New DataType** (funding rate, open interest, liquidations, options
  IV/DVOL, basis-as-a-series, on-chain metrics, sentiment index) — **M/L for
  the first one, S per additional exchange after that**. Requires: (1) a new
  `DataType` enum member, (2) a new domain record model, (3) a new
  `canonicalize()` schema branch + dedup key + storage path pattern in
  `parquet.py`, (4) a new source-protocol mixin in `sources/base.py`
  (most of these data types are REST-history-only, no live/WS need — that
  simplifies vs. OHLC/trades/book which all need live variants), (5) one
  adapter method per exchange. The repo's own roadmap already earmarks this:
  `07-roadmap.md` / `06-status.md` **Deferred — M3** lists *"Derivative
  markets — `DataType` for funding / open-interest / liquidations,
  `Symbol.market=perp`"* as a **not-started, post-3.0** epic — i.e. the
  maintainers already scoped this as one deliberate epic, not a quick add.
  Options (Deribit DVOL/IV) and on-chain/sentiment (external, non-exchange
  REST APIs) would need a further generalization: today every adapter is an
  *exchange* source keyed by `Symbol(base,quote)`; a Deribit-IV or
  CoinMetrics-on-chain source has no natural `Symbol` and would push toward a
  more generic "metric series" record type — a second new DataType shape, not
  reuse of the derivatives one.
- **New storage schema for a genuinely different shape** (e.g. an index
  series like DVOL or Fear&Greed — timestamp+value, no OHLC structure at all;
  or on-chain metrics — one value per day per chain, not per pair) — **M**:
  fits the "new DataType" mechanism above but the record model is a plain
  `(ts, value)` series rather than OHLC-shaped, so it's a genuinely new schema
  branch, not a copy-paste of `OHLCBar`.

**Bottom line**: dccd's own roadmap already flags "derivative markets"
(funding/OI/liquidations) as the natural next axis and has *not started* it —
this survey's job is to help scope that epic plus weigh it against
non-exchange sources (on-chain, sentiment, options) that need a slightly
different generalization (metric-series, not per-pair OHLC).

---

## PART 2 — source-by-source verification

(rows appended as each source is checked against official docs)


### 1. Binance USDS-M Futures — Funding Rate History
- **Source**: Binance Futures REST, `GET /fapi/v1/fundingRate`. Docs:
  https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Get-Funding-Rate-History
- **What**: historical realized funding rate per perp symbol.
- **History depth & granularity**: `startTime`/`endTime` (ms, inclusive) pagination,
  ascending order, **1000 records/request** max (default 100 if unspecified — returns
  most recent 200 with no time params per doc excerpt, functionally full-history-capable
  via paging). Funding interval **8h** (spacing between records in the response is
  28 800 000 ms) for most symbols — note some Binance perps run 4h/1h/2h dynamic
  schedules since 2023, not universal 8h (not covered by this doc page, needs a
  per-symbol check via `fundingInfo` if exact schedule matters).
- **API & limits**: shares 500 req/5min/IP with `/fapi/v1/fundingInfo`. No auth
  required for this read-only history endpoint.
- **Effort to add to dccd (vs its architecture)**: **M** (first of its kind) — new
  `DataType.FUNDING`, new `FundingRate(ts, symbol, rate)` record (trivial shape),
  new `canonicalize()` branch + storage path (e.g. `funding/{pair}/YYYY.parquet`,
  dedup on `ts`), new `FundingHistory` protocol mixin in `sources/base.py`, one
  adapter method on `BinanceSource`. **S** per additional exchange once the
  framework exists (see rows below — Bybit/OKX are structurally identical).
- **Documented alpha / intended use**: funding is the #1 documented unblock in
  fynance-research's roadmap (perp-only signal — carry/basis trades, regime filter
  for trend strategies, crowding/positioning proxy). Direct fit for ALLOC1-style
  regime-aware allocators already shipped there.
- **Priority: P0** — highest-value, well-scoped, matches an already-named roadmap
  unblock; REST-only (no WS complexity), full history via simple pagination.

### 2. Bybit v5 — Funding Rate History
- **Source**: Bybit v5 REST, `GET /v5/market/funding/history`. Docs:
  https://bybit-exchange.github.io/docs/v5/market/history-fund-rate
- **What**: historical realized funding rate per perp symbol (linear/inverse).
- **History depth & granularity**: funding interval **varies per symbol** (doc
  explicitly says "each symbol has a different funding interval" — must be read
  from `instruments-info`, not assumed 8h). `limit` param 1–200/request (default
  200); `startTime`/`endTime` pagination but **passing only `startTime` errors**
  (must pair with `endTime`) — a real pagination gotcha vs Binance's simpler
  contract. Total historical depth **not stated in this doc page** — needs an
  empirical probe (unverified here) before assuming full history is reachable.
- **API & limits**: public endpoint, no auth.
- **Effort to add to dccd**: **S** once the `DataType.FUNDING` framework exists
  (see Binance row) — one adapter method, but the paired-params pagination quirk
  needs its own edge-case handling (can't reuse a generic forward-cursor
  unchanged; small extra care, still S not M).
- **Documented alpha / intended use**: same as Binance — carry/positioning signal;
  cross-exchange funding spread is itself a documented strategy family (funding
  arb) that dccd doesn't currently support on any exchange.
- **Priority: P0** — same rationale as Binance; do together as the framework's
  first two exchanges since the marginal cost of the second is just S.

### 3. OKX v5 — Funding Rate History
- **Source**: OKX v5 Public Data REST, `GET /api/v5/public/funding-rate-history`.
  Docs: https://www.okx.com/docs-v5/en/#public-data-rest-api-get-funding-rate-history
  (docs page is a JS SPA that WebFetch could not fully extract; cross-verified by
  calling the **live endpoint directly**:
  `https://www.okx.com/api/v5/public/funding-rate-history?instId=BTC-USDT-SWAP&limit=100`
  → `code:"0"`, 100 records returned, fields `instId, instType, fundingRate,
  realizedRate, fundingTime, method, formulaType`).
- **What**: historical realized funding rate per perp (`instType=SWAP`).
- **History depth & granularity**: default/observed **100 records per call**,
  cursor pagination via `before`/`after` (ms timestamps) per docs references
  found in secondary sources — **total depth not independently confirmed** here
  (flagging as an open verification item rather than assuming full history).
  Funding interval is per-instrument (OKX has moved some pairs to non-8h
  schedules) — same caveat as Bybit.
- **API & limits**: public, no auth.
- **Effort to add to dccd**: **S** once the `DataType.FUNDING` framework exists
  (3rd exchange on the same mixin).
- **Documented alpha / intended use**: same as Binance/Bybit — cross-exchange
  funding spread requires at least 2 exchanges to be meaningful, so OKX as a 3rd
  is high value for funding-arb-style research once the first two are in.
- **Priority: P1** — same shape as P0 pair but ships third, and its total-depth
  question needs a quick empirical check before committing to "full history"
  claims in the eventual dccd capability declaration.

### 4. Binance USDS-M Futures — Open Interest Statistics (history)
- **Source**: Binance Futures REST, `GET /futures/data/openInterestHist`. Docs:
  https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Open-Interest-Statistics
- **What**: aggregated open-interest time series (contracts + notional value +
  CMC circulating supply) at fixed granularities.
- **History depth & granularity**: **9 granularities** (5m/15m/30m/1h/2h/4h/6h/
  12h/1d) but **capped at the latest 1 month** — this is a hard, documented
  limit, not a pagination artifact. Max 500 records/request (default 30).
- **API & limits**: 1000 req/5min/IP, public.
- **Effort to add to dccd**: **M** (first of its kind, distinct from funding —
  needs its own `DataType.OPEN_INTEREST` + record shape `(ts, symbol,
  sumOpenInterest, sumOpenInterestValue)`; could reuse the same "metric series"
  storage pattern being built for funding, so if funding ships first this drops
  toward S/M). Since only 1 month is retrievable, dccd would need to run this as
  an **ongoing scheduled collector** (like OHLC jobs) rather than a one-shot
  backfill — history has to be built forward from whenever collection starts,
  same operational pattern as OHLC but with a much shorter "catch-up" window.
- **Documented alpha / intended use**: OI trend/divergence vs price is a
  standard crypto-native signal (OI rising + price rising = trend confirmation;
  OI falling + price rising = short squeeze / weak hands) — complements funding
  as a positioning-crowding proxy. fynance-research's roadmap groups this with
  funding under "perp funding rates, order book/trades" unblocks.
- **Priority: P1** — real alpha, but the **1-month hard cap** means dccd must
  start collecting *now* to accumulate any history; it cannot be backfilled
  later the way funding/OHLC can. This argues for starting the collector even
  before the full framework is generalized (a strong reason to prioritize
  scheduling it early despite being P1 not P0).

### 5. Bybit v5 — Open Interest (history)
- **Source**: Bybit v5 REST, `GET /v5/market/open-interest`. Docs:
  https://bybit-exchange.github.io/docs/v5/market/open-interest
- **What**: open-interest time series per symbol.
- **History depth & granularity**: 6 granularities (5min/15min/30min/1h/4h/1d).
  **Depth: "the upper limit time you can query is the launch time of the
  symbol"** — i.e. full history back to listing, no 1-month-style cap (a real
  advantage over Binance for this data type). `limit` 1–200/page (default 50),
  cursor-paginated.
- **API & limits**: not fully specified in this doc excerpt; public endpoint.
- **Effort to add to dccd**: **S** once `DataType.OPEN_INTEREST` exists (same
  framework as Binance's OI, row 4). Because Bybit's depth is effectively
  unbounded (vs Binance's 1-month cap), Bybit is actually the **better first
  OI backfill target** if the goal is deep history rather than "start
  collecting now."
- **Documented alpha / intended use**: same as Binance OI — trend confirmation
  / squeeze detection; Bybit's full-history availability makes it usable for
  *backtesting* OI-based signals historically, not just forward monitoring.
- **Priority: P0 within the OI data type** (reprioritized above Binance OI
  specifically because full history removes the "must start collecting today"
  constraint — this is the actual best OI source to build against first).

### 6. OKX v5 — Open Interest (history)
- **Source**: OKX v5, current snapshot `GET /api/v5/public/open-interest`;
  history `GET /api/v5/trading-statistics/contract-open-interest-history`.
  Docs: https://www.okx.com/docs-v5/en/#public-data-rest-api-get-open-interest
  (JS SPA — granularity/depth/rate-limit details for the *history* endpoint
  **not extractable via WebFetch**; only the snapshot endpoint's rate limit
  confirmed: 20 req/2s per user ID). **This row is only partially verified —
  flagging explicitly rather than guessing.**
- **What**: presumed OI time series (endpoint name implies "contract open
  interest history"), granularity/depth unconfirmed.
- **Effort to add to dccd**: **S** (3rd exchange on the OI mixin) once the
  first two are built — but confirm depth/granularity before committing to a
  capability declaration (dccd's `NoCapability` discipline requires honest
  declarations, so this needs a real check, not an assumption).
- **Priority: P2** — plausible value, but do after Bybit/Binance since it's
  the least-verified of the three and OKX funding already covers OKX's
  positioning signal partially.

### 7. Binance USDS-M Futures — Long/Short Ratio (accounts + top traders) & Taker Buy/Sell Volume
- **Source**: Binance Futures REST.
  - Global accounts ratio: `GET /futures/data/globalLongShortAccountRatio`. Docs:
    https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Long-Short-Ratio
  - Taker buy/sell volume ratio: `GET /futures/data/takerlongshortRatio`. Docs:
    https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Taker-BuySell-Volume
    (confirmed directly; also cross-checked via search).
- **What**: retail/top-trader positioning ratios and aggressive-order
  (taker) buy vs sell volume — both are Binance-computed sentiment/flow
  proxies, not raw order data.
- **History depth & granularity**: **both capped at "the latest 30 days"** —
  same hard cap as Binance OI (row 4), same 9 granularities (5m…1d), same
  500 max/request, 1000 req/5min/IP, `startTime`/`endTime` optional.
- **API & limits**: public, no auth.
- **Effort to add to dccd**: **S** each, once a "metric series" DataType
  framework exists (can plausibly share the *same* new DataType/table shape as
  OI — all three are `(ts, symbol, period, value...)` Binance "futures data"
  endpoints with identical pagination contracts, so implementing OI first
  makes these two nearly free — genuinely S, arguably a single PR for all
  three Binance `/futures/data/*` endpoints).
- **Documented alpha / intended use**: classic contrarian/crowding signals
  (extreme long/short ratio, taker imbalance) used as regime filters or mean-
  reversion triggers; frequently paired with funding rate in "positioning"
  composite signals.
- **Priority: P1** — cheap to add (rides on the OI framework) but, like OI,
  **hard-capped at 30 days of history**, so value depends entirely on starting
  collection early; not useful for backtesting years of history. Bundle with
  the OI collector (same operational shape, same urgency argument).

### 8. Binance Futures — Liquidations
- **Source**: Binance Futures.
  - Market-wide liquidations (public): WS `forceOrder`/`allForceOrders` stream.
    Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Liquidation-Order-Streams
  - `GET /fapi/v1/forceOrders` (REST): confirmed **`USER_DATA`, auth-required,
    scoped to the caller's own account only** — NOT market-wide history — and
    capped at 90 days even for one's own orders. Docs:
    https://developers.binance.com/docs/derivatives/usds-margined-futures/trade/rest-api/Users-Force-Orders
- **What**: forced-liquidation events (price, qty, side, symbol).
- **History depth & granularity**: **market-wide liquidations have NO REST
  history at all** — public access is WS-stream-only, forward-only from
  connection time, and even then heavily **throttled**: "only the largest
  liquidation order within 1000ms is pushed as a snapshot" per symbol — i.e.
  Binance's public feed is a lossy sample, not a complete liquidation tape,
  even going forward.
- **API & limits**: public WS, no auth, no rate limit documented (push-based).
- **Effort to add to dccd**: **M/L** — this is dccd's **first WS-only,
  forward-only, no-backfill data type**. It doesn't fit the `history=full/
  recent` REST capability model at all; it needs the `stream`/`OHLCLive`-style
  machinery (dccd already has WS infra via `WebSocketBase`/`stream_raw()`) but
  a brand-new record type + storage path (`liquidations/{pair}/YYYY-MM-DD.
  parquet`, likely dedup on `(ts, price, qty)` since there's no exchange-
  provided id). Real effort, and the exchange's own sampling means the
  captured data is inherently incomplete even once collected.
- **Documented alpha / intended use**: liquidation cascades are a well-known
  volatility/reversal signal, but only useful in aggregate/real-time
  monitoring form given the sampling — not reconstructable historically for
  strategy backtesting beyond "however long dccd has been running the
  collector."
- **Priority: P2** — real signal exists, but (a) no way to backfill any
  history — only forward accumulation from day one, (b) exchange-side sampling
  caps data quality regardless, (c) it's a genuinely new
  architectural shape (stream-only, no REST backstop) unlike every other row
  here. Worth doing eventually, not a quick win.

### 9. Deribit — DVOL (volatility index) & options mark-price/IV
- **Source**: Deribit public JSON-RPC/REST.
  - DVOL: `public/get_volatility_index_data`. Docs:
    https://docs.deribit.com/api-reference/market-data/public-get_volatility_index_data.md
  - Per-option mark price: `public/get_mark_price_history`. Docs:
    https://docs.deribit.com/api-reference/market-data/public-get_mark_price_history.md
- **What**: DVOL is Deribit's VIX-style implied-vol index (BTC, ETH, USDC,
  USDT, EURR) as OHLC candles. Mark-price-history is per-instrument (e.g.
  `BTC-25JUN21-50000-C`) 5-minute mark price.
- **History depth & granularity**: DVOL — `start_timestamp`/`end_timestamp`
  (ms) + `resolution` ∈ {1s, 60s, 1h, 12h, 1D}, returned as OHLC candles;
  **max depth not documented** but the params imply arbitrary range query
  (DVOL launched ~March 2021, presumably queryable from there — not
  independently confirmed here). Mark-price-history is fixed **5-minute**
  granularity, **no documented time-window cap**, but explicitly **only
  covers instruments that participate in the DVOL calculation** (empty for
  futures/perps) — and it returns **mark price only, not IV directly** (IV
  would need to be derived, or read from a live ticker snapshot which is not
  historical).
- **API & limits**: both public, no auth; no rate limit documented in these
  pages.
- **Effort to add to dccd**: **M** for DVOL alone — new `DataType` (index/vol
  series), OHLC-shaped candles so it can mostly reuse the OHLC record shape
  and storage path pattern (`ohlc`-like schema keyed by a synthetic
  "instrument" = currency+"DVOL" rather than a trading pair) — genuinely the
  closest-to-existing-machinery of all the new data types surveyed. Full
  per-option **IV surface** history (strike × expiry grid over time) is a much
  bigger lift — **L** — since it requires periodic snapshotting of the live
  option chain (no historical IV-surface endpoint exists) — this is a
  different, heavier feature than DVOL and should be scoped separately if ever
  pursued.
- **Documented alpha / intended use**: DVOL as a vol-regime filter (high-vol
  vs low-vol regime switch, vol risk premium alongside realized vol from
  dccd's own OHLC) is directly complementary to fynance-research's regime-
  aware allocators (ALLOC1 already conditions on realized-vol regimes).
- **Priority: P1** — DVOL specifically is high value (regime signal, cheap
  vs. the full options surface) and closer to dccd's existing OHLC machinery
  than any other new-DataType candidate; full options IV surface is **P2/out
  of scope** for now (L effort, no clean history endpoint to backfill from).

### 10. Binance USDS-M Futures — Quarterly Futures Klines (for basis)
- **Source**: Binance Futures REST.
  - Raw per-contract klines: same `GET /fapi/v1/klines`-family endpoint as
    perpetuals, just addressed by the dated symbol (e.g. `BTCUSDT_250926`) —
    confirmed via web search (Binance FAQ + change-log references), not a
    separate API surface.
  - **Continuous** contract klines: `GET /fapi/v1/continuousKlines`, params
    `pair`, `contractType` ∈ {PERPETUAL, CURRENT_QUARTER, NEXT_QUARTER,
    (TRADFI_PERPETUAL)}, `interval`, `startTime/endTime`, `limit` (default
    500, **max 1500**). Docs:
    https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Continuous-Contract-Kline-Candlestick-Data
- **What**: OHLC klines for the **currently-active** quarterly contract,
  auto-rolled by Binance as contracts expire — i.e. a ready-made continuous
  series, no manual contract-stitching logic needed in dccd.
- **History depth & granularity**: same kline mechanics as spot/perp OHLC
  (standard intervals, `startTime`/`endTime` paging, up to 1500/request);
  depth-back-to-launch not independently confirmed but behaves like every
  other Binance klines endpoint (paginate-to-inception, same pattern dccd
  already implements for spot/perp OHLC).
- **API & limits**: weight-scaled by `limit` (1–10), same rate-limit family as
  existing OHLC calls dccd already makes.
- **Effort to add to dccd**: **S** — genuinely the cheapest new capability
  surveyed. This is **not a new DataType** at all: it's `DataType.OHLC` again,
  just against a different symbol addressing scheme (`pair` + `contractType`
  instead of a spot `Symbol`). Could likely be implemented as a second
  `capabilities()`/`fetch_ohlc_page` variant on the existing `BinanceSource`
  adapter, reusing `OHLCBar`, `canonicalize()`, and the OHLC storage path
  verbatim (perhaps `ohlc/{pair}_QUARTERLY/{span}/YYYY.parquet` or similar) —
  no new record model, no new storage schema, no new protocol mixin.
- **Documented alpha / intended use**: **basis = futures close − spot close**
  (or annualized basis %) is a textbook carry/curve signal (contango/
  backwardation regime, cash-and-carry arb signal, leading indicator for
  funding-rate direction). Directly computable in fynance-research once both
  series exist (spot OHLC already collected; this adds the futures leg).
- **Priority: P0** — best value-for-effort in the entire survey: reuses 100%
  of dccd's existing OHLC machinery, needs zero new architecture, and unlocks
  a well-documented, distinct signal family (curve/basis) that funding rate
  alone doesn't capture.

### 11. CoinMetrics Community API — on-chain fundamentals
- **Source**: `https://community-api.coinmetrics.io/v4` (no API key required).
  Docs entry: https://docs.coinmetrics.io/api/v4/ ; confirmed live by directly
  querying `GET /v4/timeseries/asset-metrics?assets=btc&metrics=AdrActCnt,
  TxCnt,SplyCur,CapMrktCurUSD&frequency=1d&page_size=5` → succeeded, returned
  daily records up to the current date (no key needed).
- **What**: on-chain + market fundamentals — active addresses (`AdrActCnt`),
  transaction count (`TxCnt`), circulating supply (`SplyCur`), market cap
  (`CapMrktCurUSD`), and others (NVT, realized cap, etc. — full catalog not
  enumerated here, would need a follow-up `catalog-all/metrics` call).
- **History depth & granularity**: **daily frequency = full history** (no
  cutoff); **hourly/minute/second frequencies limited to the last 24h** on the
  free tier (per Coin Metrics product docs, cross-checked via search). License
  is **CC BY-NC 4.0** — non-commercial only, worth flagging for a
  private-research-but-maybe-someday-monetized project.
- **API & limits**: no auth. Rate limit reported as **10 req/6s per IP** by
  the docs page fetched directly; a secondary source states **1000 req/10min**
  — same order of magnitude, treat the docs-page figure (10/6s) as primary
  since it was read directly off `docs.coinmetrics.io`.
- **Effort to add to dccd**: **M** — this is dccd's first **non-exchange**
  source: no `Symbol(base,quote)` (assets are single tickers, `btc`/`eth`),
  no OHLC shape (metrics are `(ts, asset, metric_name, value)` — a long/tidy
  table, not wide OHLC columns), REST-only (no WS/live need at all). Pushes
  toward the generic "metric series" storage shape flagged in Part 1's
  effort ladder — likely shared machinery with DVOL/on-chain/sentiment rows,
  but this would be the one that forces the abstraction to be built (first of
  its kind), so counts as **M** even though the fetch logic itself is trivial
  (single paginated GET).
- **Documented alpha / intended use**: NVT/active-addresses/supply-growth are
  classic on-chain valuation & network-health signals; fynance-research's
  roadmap explicitly names "on-chain" as one of its three documented unblocks
  (alongside funding and order book/trades).
- **Priority: P0** — free, no auth, full daily history, directly matches a
  named roadmap unblock; the main cost is architectural (first non-exchange,
  non-Symbol, non-OHLC source) rather than per-field integration work, so this
  is the single best candidate to justify building the "metric series"
  generalization.

### 12. blockchain.com Charts/Stats API — Bitcoin network metrics
- **Source**: `https://api.blockchain.info/charts/$chartName` (+ `/stats`).
  Docs referenced: https://www.blockchain.com/explorer/api/charts_api
- **What**: BTC-only network metrics — hash rate, difficulty, tx count/fees,
  mempool size, mining stats, market price, etc. (chart-name catalog not
  individually enumerated here).
- **History depth & granularity**: `timespan` param (`"5weeks"`, `"1year"`,
  or `"all"`); default is **1 year for most charts, 1 week for mempool
  charts** — must explicitly pass `timespan=all` to get full history.
  `sampled` defaults to true and **caps output at ~1.5k points** regardless of
  range (must pass `sampled=false` for true full-resolution history — verified
  live: `charts/hash-rate?timespan=all&sampled=false` returned `status:"ok"`
  starting at **1230940800 = 3 Jan 2009**, i.e. near genesis, confirming
  `timespan=all` really reaches back to the start of the chain; the "latest
  date" the fetch tool reported, ~2016, is very likely an artifact of
  WebFetch's response summarization truncating a very large JSON array rather
  than a real API cutoff — **not independently re-verified**, flagging rather
  than asserting).
- **API & limits**: no auth found in docs; no documented rate limit (unlike
  mempool.space below).
- **Effort to add to dccd**: **M**, same "metric series" shape as CoinMetrics
  (row 11) and BTC-only (no per-asset dimension needed, simplifies the record
  model further — could almost be `(ts, value)` per named metric).
- **Documented alpha / intended use**: hash rate / difficulty as a miner-
  capitulation or network-security regime signal; less directly tied to
  fynance-research's price-action strategies than funding/OI but a
  free, deep, zero-auth BTC-only complement to CoinMetrics.
- **Priority: P2** — real signal, free, deep history, but overlaps
  significantly with CoinMetrics (row 11) which is broader (multi-asset) and
  better-documented; add only if hash-rate/difficulty specifically becomes a
  research need CoinMetrics doesn't already cover.

### 13. mempool.space — mempool/fee/mining stats (public instance)
- **Source**: `https://mempool.space/api/v1/*`. Docs:
  https://mempool.space/docs/api/rest
- **What**: mempool fee estimates, difficulty-adjustment history, network
  hashrate history, mining-pool market share, block fees, and a BTC
  historical-price endpoint.
- **History depth & granularity**: time-period buckets `24h/3d/1w/1m/3m/6m/1y/
  2y/3y`, and **some endpoints accept an empty period to return "all available
  data"** — i.e. full history is reachable for at least the hashrate/mining
  endpoints.
- **API & limits**: **rate-limited with HTTP 429** on the shared public
  instance; docs explicitly steer heavy users toward **paying for an
  enterprise sponsorship** (or, implicitly, self-hosting a mempool.space
  instance) rather than hammering the free public API — a real operational
  constraint for a scheduled collector.
- **Effort to add to dccd**: **M**, same shape/effort class as blockchain.com
  (row 12); BTC-only, non-Symbol, non-OHLC metric series.
- **Documented alpha / intended use**: overlaps heavily with blockchain.com
  (hashrate, difficulty, mining) plus adds live mempool congestion/fee data —
  fee-pressure could be a BTC on-chain-activity proxy, but it's a thin,
  speculative signal for a trading strategy vs. its collection cost/rate-limit
  risk.
- **Priority: P2** — most value-add over blockchain.com is mempool fee
  data specifically; the rate-limit/self-host tension makes this a lower
  priority than CoinMetrics or blockchain.com for a small private collector.
  Skip unless mempool congestion becomes a specifically motivated signal.

### 14. DefiLlama — Stablecoins API (supply history)
- **Source**: `https://stablecoins.llama.fi/*` (Free API, no auth). Docs:
  https://api-docs.defillama.com/ ; verified live:
  `GET stablecoincharts/all` succeeded, **daily** granularity (86 400s between
  points), earliest point **29 Nov 2017** (near the start of the stablecoin
  market itself). The fetch tool's summarizer appears to have truncated the
  full response before "today" (reported stopping ~Nov 2019) — almost
  certainly a truncation artifact given the array is very large, not a real
  API cutoff; **not independently re-verified for the true end date**.
- **What**: aggregate + per-chain + per-stablecoin (USDT/USDC/DAI/…) market
  cap / circulating supply over time (`/stablecoincharts/all`,
  `/stablecoincharts/{chain}`, `/stablecoin/{asset}`), plus
  `/stablecoinprices` (peg deviation history).
- **History depth & granularity**: daily, full history back to ~2017,
  free tier, no auth; Pro tier (`pro-api.llama.fi`) exists for higher rate
  limits but isn't required.
- **API & limits**: free tier rate limit not disclosed in docs; no auth
  needed for the endpoints used here.
- **Effort to add to dccd**: **S/M** — another "metric series" source
  (non-Symbol, non-OHLC), but *very* simple shape (`ts, stablecoin/chain,
  mcap`) and a single well-documented free JSON API — if the CoinMetrics
  generalization (row 11) ships first, this is close to S.
- **Documented alpha / intended use**: aggregate stablecoin supply
  growth/contraction is a widely-used **crypto liquidity proxy** (rising
  stablecoin mcap = fresh capital entering, precedes risk-on moves; peg
  deviations flag stress events) — a genuinely different signal family from
  anything else surveyed (macro liquidity vs. per-instrument positioning).
- **Priority: P0** — free, deep (2017+), trivial shape, no auth, and a
  distinct, well-documented alpha thesis not covered by any other row here.
  High value-for-effort alongside funding and quarterly-futures basis.

### 15. alternative.me — Crypto Fear & Greed Index
- **Source**: `GET https://api.alternative.me/fng/`. Docs:
  https://alternative.me/crypto/fear-and-greed-index/
- **What**: daily composite sentiment index (0–100, volatility + momentum +
  social + dominance + surveys blended into one score).
- **History depth & granularity**: `limit` param — **`limit=0` returns "all
  available data"** (confirmed in docs), default `limit=1`. Daily granularity.
  Live-probed `?limit=0&format=json`: request succeeded and returned a large
  multi-hundred-entry array — **exact earliest/latest dates from this probe
  are unreliable** (the fetch tool's summarization garbled timestamp parsing
  on the large array, producing an internally inconsistent date range) — not
  re-verified precisely here, but the mechanism (`limit=0` = full history) is
  confirmed directly from the docs text, and the index is publicly documented
  elsewhere as running since **Feb 2018**.
- **API & limits**: no auth; no documented rate limit.
- **Effort to add to dccd**: **S** — trivial `(ts, value, classification)`
  shape, single unauthenticated GET, no pagination logic needed at all
  (one call with `limit=0` gets everything). Cheapest possible addition
  alongside quarterly-futures basis.
- **Documented alpha / intended use**: classic contrarian sentiment signal
  (extreme fear = potential bottom, extreme greed = potential top); widely
  used as a regime/overlay filter rather than a standalone signal — plausible
  cheap addition to ALLOC1-style regime conditioning.
- **Priority: P1** — near-zero effort and free, but it's a *composite,
  opaque* index (methodology not fully published) rather than a raw
  observable, and its blended nature likely correlates heavily with
  realized-vol/momentum features fynance-research already derives from OHLC —
  marginal incremental signal is the open question, not the cost.

### 16. Binance USDS-M Futures — dedicated Basis endpoint (contrast with row 10)
- **Source**: `GET /futures/data/basis`. Docs:
  https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Basis
- **What**: Binance-precomputed basis (`basis`, `basisRate`, `futuresPrice`,
  `indexPrice`) per `pair` + `contractType` (CURRENT_QUARTER/NEXT_QUARTER/
  PERPETUAL) + `period` (5m…1d) — no manual futures-minus-spot computation
  needed.
- **History depth & granularity**: **capped at "the latest 30 days"** — same
  hard cap as OI/long-short/taker-ratio (rows 4, 7). `limit` default 30, max
  500.
- **Effort to add to dccd**: **S**, and joins the same "Binance
  `/futures/data/*` metric-series batch" as rows 4 and 7 — trivially cheap
  once that framework exists.
- **Trade-off vs row 10 (quarterly-futures klines)**: this endpoint is
  *easier* (pre-computed, one call) but **shallow** (30-day cap, same
  urgency-to-start-now problem as OI); row 10 (raw continuous quarterly
  klines) is *marginally more work* (compute basis yourself from two OHLC
  series) but gets **deep history** immediately, since spot OHLC already
  exists in dccd and quarterly-futures OHLC has no documented 30-day cap.
  **Recommendation: prefer row 10** (compute basis from OHLC) as the primary
  path for research/backtesting; this dedicated endpoint is only worth adding
  later as a convenience/cross-check once live monitoring matters.
- **Priority: P2** (redundant with row 10 for research purposes; low priority
  given the better alternative already scoped).

### 17. Kraken Futures — Funding Rate History (added 2026-07-05, post-epic)
- **Source**: Kraken Futures public REST,
  `GET https://futures.kraken.com/derivatives/api/v4/historicalfundingrates?symbol=PF_XBTUSD`.
  **Verified by live probe** (2026-07-05), not just docs.
- **What**: historical realized funding per perp. **300 tradeable `PF_` linear
  perps** + 4 `PI_` inverse perps listed by `/derivatives/api/v3/instruments` —
  by far the widest perp universe of the exchanges surveyed (many alts nothing
  else covers). Two fields per entry: `fundingRate` (absolute, per contract)
  and `relativeFundingRate` (the comparable per-period rate).
- **History depth & granularity**: **hourly** cadence (vs 8h Binance/Bybit) and
  a **hard ~1-year rolling window** — probed: exactly 8 823 entries (~367 d ×
  24 h) for BOTH `PF_XBTUSD` and `PI_XBTUSD`, same start date one year back, no
  `from`/`to` params honoured → single unpaginated response, window-capped.
  Like Binance OI, history beyond a year must be accumulated forward — but the
  window is 12× wider, so urgency is months, not days.
- **API & limits**: public, no auth; one call returns the whole window.
- **Effort to add to dccd**: **S/M** — the FUNDING framework exists (v3.7.0);
  the cost is a **new adapter** `sources/kraken_futures.py` (exchange
  `krakenfutures`): Kraken Futures is a separate API surface from spot
  `KrakenSource` (different host, symbols, formats), NOT a method on the
  existing adapter. `fetch_funding_page` itself is trivial (one call, filter
  window, `next_cursor=None`); capability `history="recent"`,
  `recent_window_s≈365d`, `markets=["perp"]`. Cadence note: store
  `relativeFundingRate` in `FundingRate.rate`; research must normalise 1h vs
  8h cadences before cross-exchange comparison.
- **Documented alpha / intended use**: 3rd/4th leg of the cross-exchange
  funding spread; hourly resolution is finer regime signal; unique long-tail
  perp coverage (ADA/ATOM/UNI/… funding nothing else in dccd provides).
- **Priority: P1** — cheap on the shipped framework, distinct coverage; the
  1-year rolling window argues for starting collection within the quarter.

### 18. Kraken Futures — Open Interest (snapshot only)
- **Source**: `GET /derivatives/api/v3/tickers/{symbol}` → `openInterest`
  field (live-probed). **No history endpoint exists** on the public API.
- **What/depth**: current OI snapshot per perp; zero history.
- **Effort to add to dccd**: **S mechanically, M conceptually** — a snapshot
  source could implement `fetch_oi_page` returning a single `(now, oi)` row
  (`next_cursor=None`), accumulating rows at the recurring-job cadence — the
  same forward-capture philosophy as order-book snapshots. Needs an honest
  capability shape decision (it is neither `history="full"` nor a meaningful
  `recent_window_s`) — design note for the epic, not a blocker.
- **Priority: P2** — forward-only from day one AND snapshot-granularity only;
  do after funding + klines if at all.

### 19. Kraken Futures — Klines (charts API)
- **Source**: `GET https://futures.kraken.com/api/charts/v1/{tick_type}/{symbol}/{resolution}?from=&to=`
  (tick_type ∈ trade/mark/spot; resolutions 1m…1w). Live-probed.
- **History depth & granularity**: deep — `PI_XBTUSD` 1d reaches back to at
  least **2020-02**; `PF_XBTUSD` since launch (2022-03). Responses cap at
  **~2 000 candles** per call → window pagination (from/to), same
  `paginate_ohlc` pattern dccd already runs.
- **Effort to add to dccd**: **S** once the `kraken_futures` adapter exists
  (OHLC protocol + `markets=["perp"]`; same 12-field-free parse, different
  JSON shape). Gives Kraken-perp OHLC for the 300-perp universe + mark-price
  klines (`tick_type=mark`) as a bonus.
- **Priority: P1** — rides the same new adapter as row 17; deep history means
  no urgency, but pairs naturally with funding for perp-native research.

---

## PART 3 — Top-3 recommendation & roadmap overlap

**Top 3 by value-for-effort:**

1. **Binance perp funding rate** (row 1) + **Bybit funding rate** (row 2) as a
   pair — **P0**. Best-scoped, best-documented unblock already named in
   fynance-research's roadmap; simple REST pagination, full history, no WS
   complexity, and the framework this builds (`DataType.FUNDING`) makes OKX
   funding (row 3) and the whole Binance `/futures/data/*` family (OI,
   long/short, taker ratio, basis — rows 4,7,16) cheap follow-ons.
2. **Binance quarterly-futures klines for basis** (row 10) — **P0**. The
   single best value-for-effort item surveyed: it is *not a new DataType at
   all*, reuses `OHLCBar`/`canonicalize()`/the OHLC storage path verbatim, and
   unlocks a well-documented, architecturally-distinct signal (curve/
   contango-backwardation) that funding alone doesn't capture. Should ship
   essentially alongside/before the funding-rate work since it touches the
   *same* adapter file with *zero* new storage/domain code.
3. **CoinMetrics Community API** (row 11) — **P0**. Free, no-auth, full daily
   history, directly matches fynance-research's third named unblock
   ("on-chain"). It's the natural source to justify building dccd's second
   architectural generalization (a non-Symbol, non-OHLC "metric series"
   shape) — and once built, that same shape makes DefiLlama stablecoins
   (row 14, also P0) and Fear&Greed (row 15, P1) nearly free.

**Honorable mention / zero-effort bonus (not a new data source):** dccd
*already* collects spot OHLCV on both **Coinbase** and **Binance** — the
**Coinbase Premium Index** (Coinbase BTC-USD close − Binance BTC-USDT close,
a well-documented US-institutional-demand proxy) is computable **today**,
purely on the fynance-research side, from data dccd already has. Zero dccd
work; worth flagging to the research side directly.

**Overlap with dccd's existing roadmap** (`07-roadmap.md` / `06-status.md`,
Deferred — M3): the maintainers have already named *"Derivative markets —
`DataType` for funding / open-interest / liquidations, `Symbol.market=perp`"*
as the next axis, not started, post-3.0. This survey's rows 1–10 + 16 map
directly onto that epic and can inform its scoping (e.g. sequencing funding
before OI/liquidations, since funding is deepest-history and lowest-effort;
liquidations should probably be explicitly descoped or deferred further within
that epic given it's WS-only/forward-only/lossy-sampled — architecturally
unlike everything else in the epic). The on-chain/stablecoin/sentiment rows
(11, 14, 15) and Deribit DVOL (row 9) are **not** currently on dccd's roadmap
at all — they'd be a *new* epic (non-exchange "metric series" sources), a
distinct generalization from "derivative markets," and worth proposing as a
separate roadmap item rather than folding into the existing M3 entry.

---

## Consolidated prioritized table

| # | Source | What | History depth & granularity | API & limits | Effort (dccd) | Alpha / use | Priority + why |
|---|--------|------|------------------------------|---------------|----------------|-------------|-----------------|
| 1 | Binance USDS-M | Funding rate history | Full history (paginated), 1000/req, ~8h interval (varies) | Public, 500/5min/IP | M (1st of kind), S per extra exch. | Carry/positioning/regime signal — named roadmap unblock | **P0** — best-scoped named unblock |
| 2 | Bybit v5 | Funding rate history | Full history, per-symbol interval, 200/page cursor | Public; paired start/end required | S (once framework exists) | Same + enables funding-arb spread | **P0** — pairs with Binance, framework 2nd exch. |
| 3 | OKX v5 | Funding rate history | 100/call observed; total depth unconfirmed | Public | S | 3rd leg of funding-arb signal | **P1** — verify depth before declaring capability |
| 4 | Binance USDS-M | Open interest history | **Capped at latest 1 month**, 9 granularities | Public, 1000/5min/IP | M (1st of kind) | Trend confirmation / squeeze detection | **P1** — must start collecting now (no backfill) |
| 5 | Bybit v5 | Open interest history | **Full history to symbol launch**, 6 granularities | Public | S (2nd exch.) | Same, but backtestable (deep history) | **P0 within OI** — best OI source, no urgency constraint |
| 6 | OKX v5 | Open interest history | Unconfirmed (JS-SPA docs blocked extraction) | Partial (snapshot: 20/2s) | S (3rd exch.) | Same | **P2** — least verified, do last |
| 7 | Binance USDS-M | Long/short ratio + taker buy/sell ratio | **Capped at latest 30 days**, 9 granularities | Public, 1000/5min/IP | S (rides OI framework) | Crowding/contrarian signal | **P1** — cheap but shallow, start now |
| 8 | Binance Futures | Liquidations | **No REST history at all**; WS-only, forward-only, throttled to largest/1000ms | Public WS, no auth | M/L (new WS-only architecture) | Cascade/volatility signal, real-time only | **P2** — new architecture, lossy, no backfill |
| 9 | Deribit | DVOL index + options mark price | DVOL: arbitrary range query, 5 resolutions; mark-price: fixed 5m, DVOL-participating instruments only, no IV field | Public, no auth, no documented limit | M (DVOL, OHLC-shaped) / L (full IV surface) | Vol-regime filter, complements realized vol | **P1** — DVOL good value; full IV surface out of scope |
| 10 | Binance USDS-M | Quarterly futures klines (→ basis) | Same as spot/perp OHLC (paginate to inception), 1500/req max | Public, weight-scaled | **S — reuses OHLC machinery verbatim** | Curve/contango-backwardation carry signal | **P0** — best value-for-effort overall |
| 11 | CoinMetrics Community | On-chain + market fundamentals (multi-asset) | **Full history at daily freq**; hourly/min/sec capped to last 24h | Public, ~10 req/6s, CC BY-NC 4.0 | M (1st non-exchange source) | Named roadmap unblock ("on-chain") | **P0** — free, deep, matches roadmap |
| 12 | blockchain.com | BTC hash rate/difficulty/tx/mempool charts | `timespan=all` + `sampled=false` → full history (verified to 2009) | Public, no documented limit | M (2nd metric-series source) | Miner-capitulation / network-health regime | **P2** — overlaps CoinMetrics, BTC-only |
| 13 | mempool.space | Mempool fees, difficulty/hashrate history, mining pools | Some endpoints support "all" history | Public instance, **rate-limited (429)**, enterprise sponsorship pushed for heavy use | M | Fee-pressure/congestion proxy | **P2** — thin signal, rate-limit risk on public instance |
| 14 | DefiLlama stablecoins | Aggregate/per-chain/per-asset stablecoin supply + peg prices | Daily, full history from ~2017 | Public, free tier (Pro tier for higher limits) | S/M | Crypto liquidity proxy — distinct signal family | **P0** — free, deep, trivial shape, novel signal |
| 15 | alternative.me | Fear & Greed Index | `limit=0` = full history (documented since 2018), daily | Public, no auth | **S — trivial, one call** | Contrarian sentiment overlay | **P1** — cheapest add; composite/opaque index, marginal-signal question |
| 16 | Binance USDS-M | Dedicated Basis endpoint | **Capped at latest 30 days** (contrast row 10) | Public | S | Same as row 10 but shallow | **P2** — redundant with row 10 for research use |
| 17 | Kraken Futures | Funding rate history (300 PF perps) | **Hourly**, hard ~1-year rolling window (probed: 8823 entries), single unpaginated response | Public, no auth | S/M (new `kraken_futures` adapter; FUNDING framework exists) | 3rd funding-spread leg + widest alt-perp universe, hourly resolution | **P1** — start within the quarter (1-yr window rolls) |
| 18 | Kraken Futures | Open interest | **Snapshot only** (ticker field), no history endpoint | Public | S mech. / M design (snapshot-capture shape) | OI forward capture only | **P2** — after funding/klines, if at all |
| 19 | Kraken Futures | Perp klines (charts API, trade/mark/spot) | Deep (PI 1d ≥2020-02; PF since 2022-03), ~2000 candles/page window paging | Public | S (same new adapter) | Kraken-perp OHLC + mark-price klines | **P1** — rides the row-17 adapter |
