# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

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

## Git Flow

**Branch model:**
```
master          ← stable releases only (tagged vX.Y.Z)
  └── develop   ← integration branch
        ├── feat/<topic>   new feature or modernization axis
        ├── fix/<topic>    bug fix
        ├── chore/<topic>  tooling, CI, deps
        └── docs/<topic>   documentation only
```

**Rules — always follow these before committing or pushing:**
1. **Never commit directly to `master`.**
2. **Never commit directly to `develop`** — always use a feature branch + PR.
3. Branch off `develop`: `git checkout develop && git checkout -b feat/my-topic`
4. Open a PR into `develop` when done. `develop` → `master` only at release time.

**Commit style (Conventional Commits):**
```
feat: add Bybit futures OHLC capability
fix: correct paginator window for Coinbase
chore: upgrade httpx to 0.28
docs: update README for v3 install
```

Do not add `Co-Authored-By` trailers to commits — this is a personal repo.

**Before every commit:** run `pytest`. It must pass.

## Architecture (v3 — hexagonal)

### Three usage modes

1. **Python API** — `async with Client() as c: await c.backfill(...)`.
2. **CLI** — `dccd` command (backfill, stream, start, ui, migrate, …).
3. **HTTP API / UI** — FastAPI server + Jinja2 templates (`dccd ui` or `dccd start`).

### Package structure

```
dccd/
  domain/          # Pure, sync, zero I/O — models, capabilities, transforms
  transport/       # Async HTTP (httpx), WebSocket base, RateLimiter, Paginator
  sources/         # Exchange adapters (Source protocols + registry)
  storage/         # ParquetStore, RunsStore (SQLite), RemoteStorage, migration
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
| `parquet.py` | `ParquetStore` — read/write Parquet (ns, provenance, dedup) |
| `runs_sqlite.py` | `RunsStore` — SQLite WAL, append-only job run history |
| `remote.py` | `RemoteStorage` — rclone sync |
| `migrate.py` | `migrate_parquet_to_ns` — one-shot v2→v3 migration |

**Layout**: `{data_path}/{exchange}/ohlc/{pair}/{span}/YYYY.parquet` (annual) and `.../trades/{pair}/YYYY-MM-DD.parquet` (daily).

### Application (`application/`)

| Module | Contents |
|--------|----------|
| `config.py` | `AppConfig` + `JobConfig` — Pydantic v2, validates exchange names + span-for-OHLC |
| `events.py` | `EventBus` — pub/sub for `ProgressEvent`, `LogEvent`, `StatusEvent` |
| `jobs.py` | `JobSpec`, `JobRun`, `Trigger`, `JobParams` |
| `operations.py` | `backfill()`, `stream()`, `read()`, `inventory()` |
| `scheduler.py` | `Scheduler` — async interval/supervised/once job orchestration |
| `registry.py` | `REGISTRY` — maps operation names to schemas (parity enforcement) |
| `monitor.py` | `HealthMonitor` — EventBus subscriber, webhook alerts |
| `service_factory.py` | `build_registry()`, `build_store()`, `build_runs_store()` — **single source of truth for wiring** |

**Adding a new exchange**: add the adapter to `sources/`, register it in `service_factory.build_registry()`.

### Interfaces (`interfaces/`)

- `api/app.py` — FastAPI `create_app()`, lifespan context manager, module-level Pydantic request models
- `cli/main.py` — Typer commands, all import from `service_factory`
- `ui/` — Jinja2 templates + static files (copy from `daemon/ui/` structure)

**UI↔API contract**: UI is a pure HTTP client of the API — no direct calls to application layer.

## Testing conventions

Tests live in `dccd/tests/v3/`. No doctests (removed `--doctest-modules` from `addopts`).

Coverage is measured on every run (`--cov=dccd`). CI matrix: Python 3.11–3.13.

Key test files:
- `test_domain.py` + `test_domain_extended.py` — domain models, transforms, config validation
- `test_sources.py` — capability declarations, protocol compliance, symbol mapping
- `test_storage.py` + `test_storage_extended.py` + `test_storage_migration.py` — ParquetStore, dedup keys, v2→v3 migration round-trip
- `test_application.py` — EventBus, JobSpec, OperationRegistry parity
- `test_api.py` — FastAPI endpoints (incl. auth, backfill cancel) via TestClient
- `test_transport.py` — AsyncHTTPClient concurrency safety
- `test_backfill_lookback.py` — bounded default lookback per data type
- `test_network.py` — **real-exchange** E2E (`@pytest.mark.network`, opt-in)

**Test the chain on real data, not just the pieces.** A green unit suite missed
a backfill writing 0 rows, a store losing 58 % of trades, and a "Stop" button
that did nothing. For any data path: run the real operation, read what landed on
Parquet, and compare it to what was requested. Back up before any in-place
mutation. Full methodology + the catalogue of bugs this surfaced:
[`doc/dev/v3-testing-and-findings.md`](doc/dev/v3-testing-and-findings.md);
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

## Dependencies

Core (Python 3.11+): `httpx`, `websockets`, `pydantic>=2`, `polars`, `pyarrow`, `numpy`, `scipy`  
Daemon extra: `pyyaml`, `typer`, `tqdm`, `uvicorn`, `fastapi`, `jinja2`, `apscheduler>=3.10,<4`  
Dev extra: `pytest`, `pytest-asyncio`, `pytest-cov`, `ruff`, `mypy`, `interrogate`
