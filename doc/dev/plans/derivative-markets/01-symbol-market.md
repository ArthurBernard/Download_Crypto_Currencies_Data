---
plan: derivative-markets/01-symbol-market
kind: leaf
status: planned
complexity: medium
model: sonnet
depends: []
parallel: false
branch: feat/symbol-market
pr: ""
---

# Symbol.market + Capability.markets — the perp/futures plumbing

## Goal

`Symbol` can address non-spot markets (`perp`, `quarter`, `next_quarter`) end
to end — parse/str/storage-path/job-id — and `backfill()` rejects a market an
adapter has not declared, preserving the capability-honesty invariant. No new
data is fetched in this leaf.

## Files to change

- `dccd/domain/symbol.py` — extend `market` to
  `Literal["spot", "perp", "quarter", "next_quarter"]` (default `"spot"`);
  `__str__` returns `"BASE/QUOTE"` for spot (unchanged) and
  `"BASE/QUOTE:<market>"` otherwise; `parse()` accepts an optional
  `":<market>"` suffix (split it off *before* the separator scan; explicit
  suffix wins over the `market=` kwarg; unknown suffix → `ValueError`).
  Update the docstring examples.
- `dccd/domain/dataset.py` — `pair_slug()` returns `f"{base}-{quote}"` for
  spot (unchanged) and `f"{base}-{quote}_{market.upper()}"` otherwise
  (e.g. `BTC-USDT_PERP`, `BTC-USDT_QUARTER`).
- `dccd/domain/capability.py` — add `markets: list[str] | None = None`
  (docstring: `None` = spot-only, the honest default for every existing
  declaration) and `recent_window_s: int | None = None` (docstring: length in
  seconds of the recent window served when `history="recent"` and the window
  is time-bound rather than bar-count-bound; used by leaf 06).
- `dccd/application/operations.py` — module-level helper
  `_check_market(cap, target)` raising
  `NoCapability(target.exchange, f"{target.data_type.value}[{target.symbol.market}]", "historical")`
  when `target.symbol.market != "spot"` and
  `target.symbol.market not in (cap.markets or [])`; call it right after each
  capability lookup in the OHLC, TRADES and ORDERBOOK branches of
  `backfill()` (new branches in leaves 03/05 will call it too).
- `dccd/tests/v3/test_domain.py` — new cases (see Tests).
- `dccd/tests/v3/test_application.py` — market-rejection cases.

## Steps

1. Extend `Symbol` (validator untouched — aliases still apply to base/quote).
   `parse("BTC/USDT:perp")` → `Symbol(base='BTC', quote='USDT', market='perp')`;
   `str(...)` round-trips.
2. Extend `DatasetId.pair_slug()`; confirm `JobSpec.make_id` now
   distinguishes spot vs perp ids for free (it embeds `str(symbol)`).
3. Add the two `Capability` fields with defaults that change nothing for
   existing adapters.
4. Add `_check_market` + the three call sites in `operations.backfill`.
5. Confirm `JobConfig._validate_pairs` accepts `"BTC/USDT:perp"` (the "/"
   check already passes — add a test, no code change expected) and that
   `to_job_specs()`/`_spec_id_of` build the perp symbol via `Symbol.parse`.
6. `pytest` + `ruff check dccd/` green.

## Tests

- `test_domain.py` — parse/str round-trip for all four markets; unknown
  suffix (`"BTC/USDT:margin"`) raises; XBT alias still normalises with a
  suffix; `pair_slug()` for spot (unchanged `BTC-USDT`) and perp
  (`BTC-USDT_PERP`); `JobSpec.make_id` differs between spot and perp targets;
  `JobConfig(pairs=["BTC/USDT:perp"], data_type="ohlc", span=3600, ...)`
  validates and its spec id contains `:perp`.
- `test_application.py` — fake adapter with an OHLC capability where
  `markets=None`: backfill of a perp target fails with `NoCapability`
  *before* any fetch; with `markets=["perp"]` it proceeds. Existing suite
  stays green (spot behaviour untouched).

## Verification on real data

Spot regression only (no non-spot fetch path exists yet — leaf 02 adds it):
against an **isolated store** (temp `data_path`), run a real
`backfill binance BTC/USDT ohlc span=3600 start=<7 days ago>`, read the
Parquet back, assert ~168 hourly bars, no dup TS, path still
`binance/ohlc/BTC-USDT/1h/2026.parquet` (no suffix regression on spot slugs).

## Closeout

- CHANGELOG (`Added`): "Symbol.market (spot/perp/quarter/next_quarter) with
  `:market` pair syntax, market-aware storage slugs, and per-market
  capability declarations enforced by backfill (#NN)"
- ADR: one entry — why market lives on `Symbol` (not a new DatasetId field):
  it flows through ids, slugs and adapters for free, and the `:suffix`
  syntax keeps configs/UI single-string. Note `markets=None` = spot-only as
  the honesty-preserving default.
- Status/roadmap: deferred to leaf 07.
