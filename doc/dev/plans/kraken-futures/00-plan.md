---
plan: kraken-futures
kind: global
status: planning
roadmap: "- [ ] **Kraken Futures adapter (funding + perp klines)** — new `sources/kraken_futures.py` (separate API surface from spot `KrakenSource`)"
release_on_done: true
---

# Kraken Futures — funding (hourly) + perp klines

## Goal

dccd collects Kraken Futures **funding** (hourly cadence, 300 `PF_` linear
perps — the widest alt-perp universe surveyed) and **perp klines** (charts
API, deep history) through a new `kraken_futures` adapter, riding the
FUNDING/OHLC frameworks shipped in v3.7.0. All endpoint facts were
**live-probed 2026-07-05** — scan rows 17–19
([`../data-sources-scan-2026-07.md`](../data-sources-scan-2026-07.md)).

## Design decisions (fixed here so leaves never re-decide)

1. **New adapter, new exchange name.** `sources/kraken_futures.py`,
   `exchange = "krakenfutures"` — Kraken Futures is a separate API surface
   (host `futures.kraken.com`, `PF_` symbols, different JSON shapes) from spot
   `KrakenSource`. Registered in `service_factory.build_registry()`, added to
   `SUPPORTED_EXCHANGES` (config validation) and to `historical.html`'s
   `EXCHANGES` const (NOT `live.html` — no WS channels are implemented, and
   the UI must not offer an affordance the adapter doesn't declare).
2. **Symbol mapping**: canonical `Symbol(base, quote, market="perp")` →
   `PF_{BASE}{QUOTE}` with the Kraken alias `BTC→XBT` applied to *base* at
   render time (`Symbol.parse("XBT/USD:perp")` already normalises the other
   direction). Only `market="perp"` is declared (`markets=["perp"]`).
3. **Funding is window-capped, not paged**: one unpaginated response covering
   a hard ~1-year rolling window (probed: 8 823 hourly entries on both
   `PF_XBTUSD` and `PI_XBTUSD`). Capability: `history="recent"`,
   `recent_window_s=365*86400`. This requires the **`recent_window_s` clamp
   to be extended to the FUNDING backfill branch** in
   `application/operations.py` — leaf 05 of the previous epic added it to the
   OI branch only. `FundingRate.rate` stores `relativeFundingRate` (the
   comparable per-period rate); `fundingRate` (absolute per-contract) is NOT
   stored. **Cadence caveat for research**: Kraken funding is 1h; Binance/
   Bybit are ~8h — normalise before cross-exchange comparison (documented in
   the how-to at leaf 02).
4. **Klines via the charts API**: `GET /api/charts/v1/trade/{PF_sym}/{res}`
   with `from`/`to` in **seconds**, ascending, anchored on `from`, **~2 000
   candles max per response** (probed) → standard `paginate_ohlc` forward
   windows with `max_per_request=2000`. Resolutions 1m/5m/15m/30m/1h/4h/12h/
   1d/1w → spans `[60, 300, 900, 1800, 3600, 14400, 43200, 86400, 604800]`.
   `history="full"` (probed: `PI` 1d reaches ≥ 2020-02; `PF` since 2022-03
   launch).
5. **Out of scope (recorded, not silent)**:
   - **Open interest — P2 design note**: Kraken Futures has NO OI history
     endpoint; only a `ticker.openInterest` snapshot. Forward capture would
     mean a `fetch_oi_page` returning a single `(now, oi)` row per run —
     mechanically possible on the existing protocol, but the capability shape
     (`history`/`recent_window_s`) has no honest value for "snapshot-only".
     Revisit only if Kraken-perp OI becomes a named research need.
   - `PI_` inverse perps and `FF_`/`FI_` dated futures (no auto-rolled
     continuous series → contract-stitching dccd doesn't do).
   - Mark-price klines (`tick_type=mark` — the Capability model has no
     tick-type dimension; would need a design decision).
   - WS live channels (REST-historical epic).

## Decomposition

1. **adapter-funding** — the `kraken_futures` adapter with funding history
   end to end (wiring: registry, config, UI exchange list; operations:
   FUNDING recent-clamp), verified on real data.
2. **charts-klines-docs** — OHLC via the charts API on the same adapter +
   docs closeout (capability matrix, CLAUDE.md, how-to cadence caveat).

## Leaf checklist

- [x] 01 adapter-funding — feat/kraken-futures-funding — medium
- [ ] 02 charts-klines-docs — feat/kraken-futures-klines — medium (depends on 01)

## Dependencies

- 02 depends on 01 (same adapter file; serial — no `parallel`).

## Done criteria

- Full suite + `ruff` green at each leaf; Sphinx 0 warnings at leaf 02.
- Real-data verification recorded per leaf: funding ≈ 8.8k hourly rows per
  pair with the clamp warning firing at the declared 1-year boundary; klines
  deep backfill gap-free and byte-identical to direct API sampling.
- Honest declarations: no WS caps, no OI cap, `markets=["perp"]` only.
- Roadmap "Kraken Futures adapter" line removed at leaf 02; OI design note
  survives here and in the scan (row 18).
- **Post-release ops note**: add recurring `krakenfutures` funding jobs on
  arthurserver (BTC/ETH/SOL + the alt perps fynance-research cares about —
  Arthur picks the list) within the quarter; the 1-year window rolls.
