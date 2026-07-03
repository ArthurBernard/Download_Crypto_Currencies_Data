---
plan: derivative-markets/07-derivatives-ui-docs
kind: leaf
status: planned
complexity: medium
model: sonnet
depends: [06]
parallel: false
branch: feat/derivatives-ui-docs
pr: ""
---

# Surface derivatives in the UI + docs; epic closeout

## Goal

Funding and open-interest datasets are visible and manageable in the web UI
(Data + Historical pages), a user how-to documents the whole derivative
workflow, the dev docs reflect the new capabilities, and the epic is closed
out (roadmap line removed, descoped items re-homed, ADR journal complete).

## Files to change

- `dccd/interfaces/ui/templates/data.html` — add `Funding` and
  `Open Interest` tabs; render panes from inventory
  (`data_type in ('funding','open_interest')`); span column + coverage/gap
  cell for `open_interest` (inventory already provides
  `expected_rows`/`missing_rows` since leaf 05); no span/gap for funding.
- `dccd/interfaces/ui/templates/historical.html` — add `Funding` and
  `Open Interest` tabs (both are backfill types): per-dataset rows with
  editable `first_date`, Schedule select, Run/Delete — reuse the existing
  per-datatype rendering (`renderPane(dt)`); span handling: OI shows a span
  select constrained to the exchange's declared spans (like OHLC), funding
  has none; job-creation form accepts `:perp` pair syntax (placeholder +
  one-line hint, e.g. `BTC/USDT:perp`).
- `dccd/interfaces/api/app.py` — only if the Historical page's data needs
  it (e.g. jobs listing already exposes `data_type` generically — verify,
  expected: no change).
- `doc/source/how-to/derivatives.rst` (or the repo's how-to format) —
  collect funding (Binance/Bybit, full history), open interest (Bybit deep,
  Binance 30-day forward + why jobs must run continuously), quarterly
  futures klines + the basis recipe pointer (spot close vs `:quarter`
  close, computed research-side); `:perp`/`:quarter` symbol syntax;
  scheduling examples (config YAML + UI). Wire into the how-to toctree;
  `cd doc && make html` → **0 warnings**.
- `doc/dev/04-exchanges.md` — capability matrix: funding + OI rows for
  binance/bybit (incl. depths: Bybit funding depth as probed in leaf 04,
  Binance OI 30d), quarterly/perp klines note on the Binance row.
- `CLAUDE.md` — Architecture tables: `types.py`/`records.py` lines mention
  the new DataTypes/records; `symbol.py` line mentions markets; Historical
  page description "OHLC + Trades only" updated to include Funding + OI.
- `doc/dev/07-roadmap.md` — **remove** the "Derivative markets" bullet;
  add one follow-on line under Deferred — M3: "OKX funding/OI — cheap
  follow-ons on the FUNDING/OPEN_INTEREST mixins; verify history depth
  before declaring capabilities (scan rows 3/6)"; append to the
  metric-series bullet: "…also Binance long/short + taker ratios (scan
  row 7) — same 30-day cap as OI, time-sensitive."
- `doc/dev/06-status.md` — status entry for the shipped epic.
- `doc/dev/03-decisions.md` — closing ADR: liquidations **descoped**
  (WS-only, forward-only, lossy sampling — tombstone with the scan
  reference) + pointer to the per-leaf ADR entries.

## Steps

1. UI: Data tabs → Historical tabs → manual click-through.
2. UI smoke: run `doc/dev/ui_smoke.py` against an isolated `dccd ui`
   seeded with leaf 03/05 verification data; fix any console/JS errors.
3. Docs: how-to + capability matrix + CLAUDE.md; `make html` 0 warnings.
4. Closeout edits: roadmap/status/ADR (this leaf carries the epic-level
   closeout that other leaves deferred).
5. `pytest` + `ruff check dccd/`.

## Tests

- `test_api.py` — `GET /api/jobs` exposes funding/OI jobs with their
  fields; job create/delete round-trip for a funding and an OI job (span
  rules per leaf 05).
- UI: `ui_smoke.py` green (headless chromium), zero console errors on
  Data/Historical with derivative datasets present.

## Verification on real data

Isolated `dccd ui` on a store containing real leaf-03/05/06 verification
output (funding + OI + quarterly OHLC):

1. Data page: both new tabs list the datasets with correct rows/from/to;
   OI shows coverage; funding shows none.
2. Historical page: create a funding job (`bybit BTC/USDT:perp`) via the
   UI → appears in config; Run → rows actually land on disk (read the
   Parquet); Delete → gone from config and UI.
3. Quarterly OHLC dataset (`BTC-USDT_QUARTER`) renders correctly on the
   existing OHLC tab (slug displays, coverage sane).

## Closeout

- CHANGELOG (`Added`): "Data & Historical UI tabs for funding and open
  interest; how-to guide for derivative-markets collection (#NN)"
- ADR: the closing entry described in Files (liquidations tombstone).
- Status/roadmap: THIS leaf applies them (see Files); epic global
  `00-plan.md` → `status: done`; suggest `/release` (release_on_done) and
  relay the **prod ops note**: start OI jobs on arthurserver immediately
  after deploy.
