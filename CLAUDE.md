# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

> **Claude-oriented developer brief**: [`doc/dev/`](doc/dev/) contains an
> orientation pack written specifically for Claude Code — overview, architecture,
> design decisions & rationale, the per-exchange capability matrix, testing
> methodology + findings, current status, and the roadmap. Start at
> [`doc/dev/README.md`](doc/dev/README.md) for a fuller picture than this file
> gives. `CLAUDE.md` remains authoritative for commands and invariants.

## Common conventions

<!-- mirror of ~/.claude/CLAUDE.md — synced 2026-07-04 -->

Shared across my repos, mirrored from `~/.claude/CLAUDE.md` (the single source of
truth — if they ever disagree, the global file wins). Restated here so the repo
stays self-contained:

- **Git Flow** — `master` (tagged releases) ← `develop` (integration) ←
  `feat|fix|chore|docs/<topic>`. **Never commit directly to `develop` or `master`**
  — always a feature branch + PR into `develop`; `develop` → `master` only at release.
- **Conventional Commits** — `feat:` `fix:` `chore:` `docs:`. **Never add
  `Co-Authored-By` trailers** (personal repo).
- **One PR = one concern**, small and disposable — a big plan ships as several small
  atomic PRs, never one catch-all branch.
- **Model: the session model, always** — interactive sessions and every spawned
  subagent inherit it (set in `~/.claude/settings.json`); a plan leaf's
  `complexity` is effort/ordering only and never selects or downgrades the model.
- **Before every commit** — `pytest` and `ruff check dccd/` must pass.

## Commands

```bash
# Dev install (Python 3.11+)
pip install -e ".[dev]"

# Run full unit suite (network E2E excluded by default via -m 'not network')
pytest

# Run a single test file
pytest dccd/tests/v3/test_domain.py -v

# Real-exchange end-to-end tests (hit live APIs; opt-in)
pytest -m network

# Lint
ruff check dccd/

# Type check (strict on domain/; mypy assumes python 3.12 — see note below)
mypy dccd/

# Build Sphinx docs (must produce 0 warnings)
cd doc && make html

# UI smoke test (headless browser; start an isolated `dccd ui` first)
pip install playwright && playwright install chromium
python doc/dev/ui_smoke.py http://127.0.0.1:8137
```

> **mypy** assumes `python_version = 3.12` (in `pyproject.toml`): the dev/docs
> env ships Sphinx whose source uses 3.12 `type` statements, which made mypy
> abort under 3.11. dccd supports 3.11–3.13, so 3.12 semantics are safe.

## Dev loop & docs of record

The iterative loop is tooled by skills, with four tracked docs as the sources of
truth:

| Doc | Holds | Updated by |
|-----|-------|-----------|
| `doc/dev/07-roadmap.md` | open work — single source *index* | `/pick-task` reads · `/finish-task`, `/abandon-task` update |
| `doc/dev/plans/<epic>/` | open work *detail* — durable hierarchical plan trees (global + leaf specs) | `/plan` writes · `/execute-leaf` reads · `/finish-task`/`/abandon-task` archive |
| `doc/dev/03-decisions.md` | the *why* — ADR journal (+ settled rationale) | `/finish-task` (accepted), `/abandon-task` (rejected/tombstone) |
| `doc/dev/06-status.md` | where things stand | `/finish-task`, `/groom-docs` |

`CHANGELOG.md` + git log stay authoritative for *what* shipped. The loop:

`/pick-task` (smallest coherent slice; **no branch yet**) →
`/plan` (decompose into a `doc/dev/plans/<epic>/` tree — adaptive depth: a single
leaf for a trivial task, a global `00-plan.md` + leaves otherwise — and open the
**plan PR** that lands the tree on `develop` first) →
`/execute-leaf <epic> next` (cut the leaf branch, **spawn an agent — session
model, effort derived from the leaf's `complexity`** — which implements + tests +
**verifies on real data**, then reports) →
`/finish-task` (tests, ADR, CHANGELOG, leaf PR, archive the leaf, tick the global
checklist) → … per leaf … → last leaf removes the roadmap line → `/release`.

`/abandon-task` salvages the lesson + closes a bad PR (tombstones the leaf);
`/groom-docs` periodically keeps `doc/dev/` lean and true. The full format lives in
[`doc/dev/plans/README.md`](doc/dev/plans/README.md). The workflow is
backward-compatible: a repo whose `.claude/workflow.json` has **no `plans_dir`**
falls back to the older `/pick-task → plan mode → /finish-task` loop.

## Architecture (v3 — hexagonal)

### Three usage modes

1. **Python API** — `async with Client() as c: await c.backfill(...)`.
2. **CLI** — `dccd` command (backfill, stream, start, ui, …).
3. **HTTP API / UI** — FastAPI server + Jinja2 templates (`dccd ui` or `dccd start`).

### Package structure

```
dccd/
  domain/          # Pure, sync, zero I/O — models, capabilities, transforms
  transport/       # Async HTTP (httpx), WebSocket base, RateLimiter, Paginator
  sources/         # Exchange adapters (Source protocols + registry)
  storage/         # ParquetStore, RunsStore (SQLite), RemoteStorage
  application/     # Operations (backfill, stream), Scheduler, EventBus, Config
  interfaces/
    api/           # FastAPI app (1:1 with OperationRegistry)
    cli/           # Typer CLI (asyncio.run)
    ui/            # Jinja2 templates (pure HTTP client of api/)
  tests/v3/        # All tests
```

### Domain layer (`domain/`)

Pure, synchronous, no I/O. Never import from transport/sources/storage.

| Module | Contents |
|--------|----------|
| `symbol.py` | `Symbol(base, quote)` — normalises XBT→BTC |
| `types.py` | `DataType` enum: `ohlc`, `trades`, `orderbook` |
| `records.py` | `OHLCBar`, `Trade`, `OrderBookSnapshot` (ns timestamps) |
| `dataset.py` | `DatasetId`, `Provenance` |
| `capability.py` | `Capability` — declared per adapter per (data_type × transport × mode) |
| `timeutils.py` | Helpers: `s_to_ns`, `align_ns`, `span_label`, `binance_interval`, … |
| `transforms.py` | `aggregate_ohlc(trades, span)` — pure computation |
| `errors.py` | `NoCapability`, `CoverageError` |

**All internal timestamps are nanoseconds UTC (int64).**

### Transport layer (`transport/`)

Async only. Drives I/O; domain stays pure.

| Module | Contents |
|--------|----------|
| `http.py` | `AsyncHTTPClient` — httpx wrapper with retry/backoff |
| `ws.py` | `WebSocketBase` — `stream_raw()` async generator with exponential reconnect |
| `ratelimit.py` | `RateLimiter` — token-bucket per exchange |
| `paginate.py` | `paginate_ohlc`, `paginate_trades` — generic forward paginator |

**Paginator contract**: callers must pass a closure with `symbol` (and `span` for OHLC) already bound:

```python
async def _fetch(start_ns, end_ns, limit):
    return await adapter.fetch_ohlc_page(symbol, span, start_ns, end_ns, limit)
async for bar in paginate_ohlc(_fetch, cap, start_ns, end_ns, span):
    ...
```

### Source adapters (`sources/`)

One class per exchange implementing Source protocol mixins:

- `OHLCHistory`, `TradesHistory`, `OrderBookSnapshotREST` — REST historical
- `OHLCLive`, `TradesLive`, `OrderBookLive` — WebSocket live

Adapters declare their capabilities via `capabilities() -> list[Capability]`.

| Exchange | Notes |
|----------|-------|
| `binance.py` | Full history OHLC+trades, depth 5000 |
| `coinbase.py` | 300 candles/req (Paginator handles automatically) |
| `kraken.py` | OHLC: 720 recent only (`history="recent"`); trades: full via `since` cursor |
| `bybit.py` | No spot trades history (capability not declared → `NoCapability` early) |
| `okx.py` | `history-candles` + `history-trades` for deep history |
| `bitfinex.py` | Up to 10 000 items per request |
| `bitmex.py` | Bucketed OHLC (1m/5m/1h/1d only), full trades |

**WS adapters** extend `WebSocketBase` and use `self.stream_raw()` (NOT a custom `_stream_raw` — the base handles reconnect).

### Storage (`storage/`)

| Module | Contents |
|--------|----------|
| `parquet.py` | `ParquetStore` — read/write Parquet (ns, provenance, dedup); `inventory()` enriched with on-disk `bytes` and (OHLC only) `expected_rows`/`missing_rows` gap detection at zero extra I/O |
| `runs_sqlite.py` | `RunsStore` — SQLite WAL, append-only job run history |
| `remote.py` | `RemoteStorage` — rclone sync |

**Layout**: `{data_path}/{exchange}/ohlc/{pair}/{span}/YYYY.parquet` (annual) and `.../trades/{pair}/YYYY-MM-DD.parquet` (daily).

### Application (`application/`)

| Module | Contents |
|--------|----------|
| `config.py` | `AppConfig` + `JobConfig` — Pydantic v2, validates exchange names + span-for-OHLC; runtime CRUD (`add_job`, `remove_job`, `update_job_start`) normalises mutations to single-pair entries (multi-pair configs are read but split on edit) |
| `events.py` | `EventBus` — pub/sub with **multi-queue fan-out** (`add_queue`/`remove_queue`, `enable_queue` alias) so Live + Logs + Dashboard consume concurrently; events: `ProgressEvent`, `LogEvent`, `StatusEvent`, `StreamSampleEvent` |
| `jobs.py` | `JobSpec`, `JobRun`, `Trigger`, `JobParams` |
| `operations.py` | `backfill()`, `stream()` (emits throttled `StreamSampleEvent` ≤1/s for Live liveness), `read()`, `inventory()` |
| `scheduler.py` | `Scheduler` — async interval/supervised/once job orchestration; `sync_streams()` reconciles stream workers and `sync_intervals()` reconciles recurring backfill loops (start/cancel/restart on cadence change, keyed by spec id) — both stop+drop deleted ones |
| `registry.py` | `REGISTRY` — maps operation names to schemas (parity enforcement) |
| `monitor.py` | `HealthMonitor` — EventBus subscriber, webhook alerts |
| `service_factory.py` | `build_registry()`, `build_store()`, `build_runs_store()` — **single source of truth for wiring** |

**Adding a new exchange**: add the adapter to `sources/`, register it in `service_factory.build_registry()`.

### Interfaces (`interfaces/`)

- `api/app.py` — FastAPI `create_app()`, lifespan context manager, module-level Pydantic request models. Job CRUD lives here: `POST /api/jobs/{create,delete,update}` (body-based to allow `/`/`:` in ids), all routed through the async `_persist_and_refresh` helper (writes YAML, updates `app.state`, calls `scheduler.sync_streams` **and** `scheduler.sync_intervals` to reconcile recurring backfills live). `POST /api/jobs/update` edits `start` and/or the recurring `every` (schedule). `GET /api/jobs` exposes `start`/`every`/`trigger`/`snapshot_interval`/`depth` so the UI can render and preserve them. `POST /api/jobs/run` + `/api/jobs/run-all` trigger configured backfills on demand. SSE at `GET /api/events` uses `add_queue`/`remove_queue` for multi-consumer fan-out.
- `cli/main.py` — Typer commands, all import from `service_factory`
- `ui/` — Jinja2 templates + static files. Nav: `Dashboard` · `Data` flat, plus `Collect ▾` (Historical/Live) and `System ▾` (Logs/Config/Storage) dropdowns. Pages are **split by concern**:
  - **Data** (`data.html`, route `/data`; `/inventory` 307-redirects here) — read-only view of what's on disk: DataType tabs → per-exchange accordions with totals, freshness dot, OHLC gap %, on-disk size, file count. No action buttons.
  - **Historical** (`historical.html`) — backfill jobs (**OHLC + Trades only**; order books have no REST history): DataType tabs → exchange accordions → one row per dataset with editable `first_date` (defaults to the dataset's earliest stored bar), a **Schedule** select (Off/hourly/daily/custom → `every`; `manual` trigger when off), real coverage bar, inline Run/Delete. **Run all** (header) + per-exchange **Run all**. New jobs default to `manual`.
  - **Live** (`live.html`) — stream jobs (**Trades + Order Book only**; OHLC is collected via the Historical schedule, not streamed): same tab/accordion shape, with a liveness indicator fed by `StreamSampleEvent` over SSE (numeric `value`/`bid`/`ask`, formatted client-side via `fmtNum`). Liveness is **seeded from the last on-disk point** (inventory `max_ts`) so a refresh shows freshness without waiting for a live sample. The dot's "fresh" window is span-aware (order-book `snapshot_interval` / short for trades); the freshness label is a relative "N ago" under 24h (`fmtFreshness`) and an absolute date beyond, or the last-run date-time when stopped. Cadence column + `snapshot_interval` field for order book. Inline Start/Stop/Delete.
  - Single top bar carries the brand (logo · `dccd` · version) left and the nav right. Dates render in `settings.timezone` (`local`/`UTC`/zoneinfo) via `DCCD_TZ` in `fmtNs`/`fmtDate`; relative ages are tz-independent.
  - `dashboard.html` (KPIs + Active now / Recent runs / Data), `logs.html` (recent runs first, live console secondary, human run labels), `config.html` (Settings incl. `timezone`/Alerts/Storage + Raw JSON — **no jobs form**; jobs are managed on Historical/Live), `storage.html` (sizes via `fmtBytes`; no migrate tool).

**UI↔API contract**: UI is a pure HTTP client of the API — no direct calls to application layer. Inline job create/edit/delete on Historical/Live go through `/api/jobs/*`; the Config page no longer manages jobs (edit the `jobs` array via its Raw JSON tab if needed).

## Testing conventions

Tests live in `dccd/tests/v3/`. No doctests (removed `--doctest-modules` from `addopts`).

Coverage is measured on every run (`--cov=dccd`). CI matrix: Python 3.11–3.13.

Key test files:
- `test_domain.py` + `test_domain_extended.py` — domain models, transforms, config validation
- `test_sources.py` — capability declarations, protocol compliance, symbol mapping
- `test_storage.py` + `test_storage_extended.py` — ParquetStore, dedup keys, gap detection
- `test_application.py` — EventBus (multi-queue fan-out, `sample`), JobSpec, OperationRegistry parity, `AppConfig` job CRUD (incl. multi-pair split)
- `test_api.py` — FastAPI endpoints (incl. auth, backfill cancel, `/api/jobs/{create,delete,update}`, stream-delete unregisters worker) via TestClient
- `test_transport.py` — AsyncHTTPClient concurrency safety
- `test_backfill_lookback.py` — bounded default lookback per data type
- `test_network.py` — **real-exchange** E2E (`@pytest.mark.network`, opt-in)

**Test the chain on real data, not just the pieces.** A green unit suite missed
a backfill writing 0 rows, a store losing 58 % of trades, and a "Stop" button
that did nothing. For any data path: run the real operation, read what landed on
Parquet, and compare it to what was requested. Back up before any in-place
mutation. Full methodology + the catalogue of bugs this surfaced:
[`doc/dev/05-testing.md`](doc/dev/05-testing.md);
UI smoke test: `doc/dev/ui_smoke.py`.

### Invariants — do not regress

- **Trades pagination is cursor-based** (per-adapter opaque cursor); never
  advance trades by a fixed time window. OHLC snaps the start to the *bar* (span),
  not the window.
- **Dedup key is per data type** (`ParquetStore._dedup_subset`): OHLC=`TS`,
  trades=`tid`(else composite), order book=`(TS,side,price)`. `TS` alone is
  unique only for OHLC.
- **Declared capabilities must be honest**: don't declare a WS channel or
  `history` depth that isn't implemented — the engine rejects undeclared ones.
- **All timestamps ns UTC int64**; legacy frames pass through
  `ParquetStore.canonicalize()` before any `concat`.
- **First `start=last` backfill is bounded** per type (`_DEFAULT_LOOKBACK_NS`);
  backfills are cancellable (`stop_event` → `DELETE /api/backfill/{id}`).
- **Adapters share one reference-counted HTTP client** (concurrency-safe).
- **`ui_auth_token` enforces Bearer on `/api/*`**; CORS is not wildcard.
- **Stream worker set is reconciled, not append-only**: deleting a stream job must
  `Scheduler.sync_streams()` so its worker is stopped and dropped (never left
  running/controllable after its config is gone).
- **`EventBus` fans out to all registered queues**; SSE consumers register via
  `add_queue` and must `remove_queue` on disconnect (done in the `/api/events`
  `finally`).

## Dependencies

Core (Python 3.11+): `httpx`, `websockets`, `pydantic>=2`, `polars`, `pyarrow`, `numpy`, `scipy`  
Daemon extra: `pyyaml`, `typer`, `tqdm`, `uvicorn`, `fastapi`, `jinja2`, `apscheduler>=3.10,<4`  
Dev extra: `pytest`, `pytest-asyncio`, `pytest-cov`, `ruff`, `mypy`, `interrogate`
