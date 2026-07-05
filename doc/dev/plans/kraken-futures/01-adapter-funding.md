---
plan: kraken-futures/01-adapter-funding
kind: leaf
status: planned
complexity: medium
depends: []
parallel: false
branch: feat/kraken-futures-funding
pr: ""
---

# Kraken Futures adapter + hourly funding history

## Goal

`backfill krakenfutures "BTC/USD:perp" funding` lands the full available
funding window (~1 rolling year, hourly) on disk, with the clamp warning
firing at the declared boundary when a deeper start is requested. New
adapter, first capability.

## Files to change

- `dccd/sources/kraken_futures.py` (NEW) — `KrakenFuturesSource(FundingHistory)`:
  - `exchange = "krakenfutures"`; `__init__` like siblings
    (`self._http = http or default_http_client(self.exchange)`).
  - `_BASE = "https://futures.kraken.com/derivatives/api"`.
  - `render_symbol(s)`: `PF_{ALIAS(base)}{quote}` with `_KRAKEN_ALIASES =
    {"BTC": "XBT"}` applied to base (e.g. `BTC/USD:perp` → `PF_XBTUSD`,
    `SOL/USD:perp` → `PF_SOLUSD`).
  - `fetch_funding_page(symbol, start_ns, end_ns, limit, cursor=None)`:
    GET `{_BASE}/v4/historicalfundingrates` with
    `{"symbol": self.render_symbol(symbol)}` — no other params (the endpoint
    ignores them; single response). Check `result == "success"` (log error +
    `([], None)` otherwise). Entries are ascending
    `{"timestamp": ISO-8601 Z, "fundingRate": float, "relativeFundingRate":
    float}` → `FundingRate(ts=<ISO parsed to ns UTC>, rate=
    relativeFundingRate)` (use `datetime.fromisoformat` handling the `Z`
    suffix; multiply to ns int). Filter to `[start_ns, end_ns]` in the
    adapter (cheap; the paginator filters again — harmless). Always return
    `next_cursor=None` (one page is the whole window). Ignore `limit` and
    `cursor` (document why in the docstring).
  - `capabilities()`: `[Capability(data_type=DataType.FUNDING,
    transport="rest", mode="historical", history="recent",
    recent_window_s=365 * 86400, max_per_request=10000,
    page_direction=None, markets=["perp"])]` — nothing else (no WS, no
    OHLC yet, no OI: honesty invariant).
  - Class docstring: numpydoc like siblings; note the separate-API-surface
    rationale and the 1h cadence.
- `dccd/application/service_factory.py` — import + `reg.register(
  "krakenfutures", KrakenFuturesSource())` (build_registry).
- `dccd/application/config.py` — add `"krakenfutures"` to
  `SUPPORTED_EXCHANGES`.
- `dccd/application/operations.py` — extend the **FUNDING branch** with the
  same recent-window clamp the OI branch has (leaf 05 pattern): if
  `cap.history == "recent"` and `cap.recent_window_s`, clamp `start_ns` to
  `end_ns - cap.recent_window_s * NS` when older, with the same warning
  wording. (Binance/Bybit funding declare `history="full"` — behaviour
  unchanged for them; add a regression assertion.)
- `dccd/interfaces/ui/templates/historical.html` — add `'krakenfutures'` to
  the `EXCHANGES` const (line ~40). Do NOT touch `live.html` (no WS caps —
  the UI must not offer it).
- `dccd/tests/v3/test_sources.py`, `test_application.py`, `test_domain.py`
  — see Tests.

## Steps

1. Adapter file (symbol rendering + funding fetch + capability).
2. Wiring: service_factory, SUPPORTED_EXCHANGES, historical.html EXCHANGES.
3. Operations: FUNDING recent-clamp (mirror the OI branch block).
4. `pytest` + `ruff check dccd/` green (full suite).

## Tests

- `test_sources.py` — new `TestKrakenFuturesFunding` (stubbed client):
  `render_symbol` (`BTC/USD:perp`→`PF_XBTUSD`, `ETH/USD:perp`→`PF_ETHUSD`);
  request URL/params (`symbol` only); ISO-Z timestamp → ns conversion exact;
  `rate == relativeFundingRate` (NOT `fundingRate`); window filter; always
  `next_cursor=None`; `result != "success"` → `([], None)`; capability
  honesty (`history="recent"`, `recent_window_s=365*86400`,
  `markets=["perp"]`, sole capability in the list).
- `test_application.py` — FUNDING clamp: fake adapter with
  `history="recent"`, `recent_window_s=365*86400` → `start=origin` clamps to
  `end − 365d` with the warning; a `history="full"` funding adapter is NOT
  clamped (Binance/Bybit regression).
- `test_domain.py` (or config tests) — `JobConfig(exchange="krakenfutures",
  data_type="funding", pairs=["BTC/USD:perp"])` validates; unknown exchange
  still rejected.

## Verification on real data

Isolated store (scratchpad temp `data_path`), real Kraken Futures API:

1. `backfill krakenfutures "BTC/USD:perp" funding start=origin` — the clamp
   warning must fire (quote it) and ≈ 8 800 hourly rows land
   (`krakenfutures/funding/BTC-USD_PERP/{2025,2026}.parquet`); TS spacing
   3600 s (modal), 0 duplicate TS; earliest TS ≈ now − 365 d.
2. Same for `ETH/USD:perp` and `SOL/USD:perp` (≈ 8.8k rows each).
3. Cross-check 5 sampled `relativeFundingRate` values against a direct
   `curl` (exact match); sanity: |rate| < 1e-3 hourly.
4. Re-run `start=last` — idempotent (≤ 1 new row).

## Closeout

- CHANGELOG (`Added`): "Kraken Futures adapter (`krakenfutures`) — hourly
  perp funding history over its ~1-year rolling window (`history='recent'`
  + clamp, 300 PF_ perps addressable) (#NN)"
- ADR: one entry — the FUNDING recent-clamp generalisation (was OI-only) +
  storing `relativeFundingRate` (comparable rate) and dropping the absolute
  `fundingRate` field; separate-adapter decision (vs a method on spot
  `KrakenSource`).
- Status/roadmap: deferred to leaf 02.
