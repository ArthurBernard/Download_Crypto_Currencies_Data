---
plan: kraken-altname-pairs/01-kraken-altname-pairs
kind: leaf
status: planned
complexity: medium
depends: []
parallel: false
branch: fix/kraken-altname-pairs
pr: ""
---

# Map Kraken pairs by altname so modern assets (TRX/DOT/BNB/DOGE…) work

## Goal
`_kraken_pair` currently emits legacy X/Z-prefixed Kraken codes
(`X{base}Z{quote}` / `X{base}X{quote}`), which only Kraken's *old* assets use.
Modern assets (TRX, DOT, BNB, …) and Dogecoin (Kraken code `XDG`) get
"Unknown asset pair" on every REST call. Switch to Kraken **altnames**
(`{base}{quote}` with `BTC→XBT`, `DOGE→XDG`), which Kraken accepts for *all*
assets — fixing OHLC backfill and trades history for these pairs.

## Context
Requested: collect Kraken OHLC for XRP, LTC, ZEC, DOGE, XLM, TRX, BCH, BNB, DOT
(USD + /BTC cross). Live-tested via `KrakenSource.fetch_ohlc_page`: XRP, LTC,
ZEC, XLM, BCH work; **TRX, DOT, BNB, and DOGE fail** with `EQuery:Unknown asset
pair`. No config workaround exists — the legacy formula cannot produce `TRXUSD`.
Verified against the live REST API that altnames work for everything:
`TRXUSD, TRXXBT, DOTUSD, DOTXBT, BNBUSD, XDGUSD, XDGXBT` all return data, and the
legacy `XRPUSD`/`XBTUSD` also work (Kraken keys the *result* by its internal code
— `XXRPZUSD`, `XXDGXXBT` — which the adapters already tolerate, see below).

## Design decision
Use the **altname** form, not a hardcoded full asset→code map and not a runtime
`AssetPairs` lookup. Kraken accepts altnames universally for the `pair` request
param, and **both** `fetch_ohlc_page` and `fetch_trades_page` already parse the
response with a code-key fallback (`result_data.get(pair, result_data.get(<first
non-"last" key>, []))`), so they don't depend on `pair` matching the response key.
Only two asset aliases are needed for these pairs: `BTC→XBT` (Kraken's name for
Bitcoin, base *and* quote) and `DOGE→XDG`.

## Files to change
- `dccd/sources/kraken.py`:
  - Replace the body of `_kraken_pair` with the altname construction. Add a small
    module-level alias map, e.g. `_KRAKEN_ALIASES = {"BTC": "XBT", "DOGE": "XDG"}`,
    and return `f"{base}{quote}"` after aliasing both sides. This subsumes the old
    `BCH`/`DASH` special case (they were already prefix-less) and the BTC→XBT
    handling.
  - Update the two doctests in `_kraken_pair`'s docstring: `BTC/USD → 'XBTUSD'`,
    `ETH/BTC → 'ETHXBT'`.
  - Leave `fetch_ohlc_page` / `fetch_trades_page` parsing **unchanged** (the
    code-key fallback already handles altname requests). Leave `_ws_pair`
    unchanged (WebSocket uses friendly `BASE/QUOTE`; out of scope — the requested
    collection is OHLC backfill, not streams). If a quick check shows Kraken WS v2
    needs `XDG/USD` for Dogecoin, note it in the PR but do **not** expand scope.

## Steps
1. Edit `_kraken_pair` to alias `BTC→XBT` and `DOGE→XDG` on both base and quote,
   then return `f"{base}{quote}"`. Keep it a pure function.
2. Update the docstring doctests to the new expected outputs.
3. `ruff check dccd/` and `mypy dccd/` clean.

## Tests
- `dccd/tests/v3/test_sources.py` — add/extend a `_kraken_pair` test asserting:
  - legacy: `BTC/USD→XBTUSD`, `ETH/BTC→ETHXBT`, `XRP/USD→XRPUSD`, `XRP/BTC→XRPXBT`;
  - modern: `TRX/USD→TRXUSD`, `DOT/BTC→DOTXBT`, `BNB/USD→BNBUSD`;
  - DOGE alias: `DOGE/USD→XDGUSD`, `DOGE/BTC→XDGXBT`.
- Run the full suite (`pytest`) — the existing Kraken tests must stay green
  (mapping output changed from `XXBTZUSD`→`XBTUSD` etc.; update any test/fixture
  that asserted the old code form).

## Verification on real data
Per the `data-e2e` discipline (network — hits live Kraken):
1. Via `KrakenSource.fetch_ohlc_page` (span 60, recent window), fetch **legacy**
   `BTC/USD`, `ETH/BTC`, `XRP/USD` **and modern** `TRX/USD`, `DOT/USD`, `BNB/USD`,
   `DOGE/USD`, `DOGE/BTC`. Assert each returns **> 0 bars** with a plausible close.
2. Spot-check one trades fetch (`fetch_trades_page`) for a modern asset
   (e.g. `TRX/USD`) returns trades, confirming the trades path also works.
3. Record the per-pair bar counts in the PR description.

## Closeout
- CHANGELOG (`Fixed`): "Kraken adapter maps pairs by altname (`{base}{quote}`,
  `BTC→XBT`, `DOGE→XDG`) instead of legacy X/Z-prefixed codes, so OHLC/trades for
  modern Kraken assets (TRX, DOT, BNB, …) and Dogecoin no longer fail with
  `Unknown asset pair`. (#NN)"
- ADR (`doc/dev/03-decisions.md`): altname over hardcoded map / runtime
  `AssetPairs`; rationale — Kraken accepts altnames universally and the response
  parsing already falls back to the code-key, so no lookup is needed.
- Status/roadmap: no roadmap line (ad-hoc, enables the multi-asset Kraken
  collection request) — `/finish-task` adds ADR + CHANGELOG.
