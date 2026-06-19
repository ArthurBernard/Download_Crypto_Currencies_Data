# 1 — Overview

## What dccd is

**dccd** (Download Crypto-Currencies Data) collects market data — OHLC candles,
trades, and order books — from seven spot exchanges and stores it locally as
Parquet. It does **historical backfill** (REST, paginated) and **live streaming**
(WebSocket), with no API key required (public endpoints only).

Three ways to drive it, all over the same core:

| Mode | Entry point | Use |
|------|-------------|-----|
| **Python API** | `async with Client() as c: await c.backfill(...)` | embed in code / notebooks |
| **CLI** | `dccd` (Typer) — `backfill`, `stream`, `start`, `ui`, `inventory`, … | scripting / ops |
| **HTTP API + Web UI** | `dccd ui` (UI+API) or `dccd start` (full daemon) | FastAPI + Jinja2, browser dashboard |

## Current state (snapshot)

- **Version `3.6.0`** (in `pyproject.toml`), released on `master` and published to
  PyPI (2026-06-13). The v3 work is a full hexagonal rewrite that replaced the v2
  daemon; the v3 line began at `v3.0.0` (2026-06-07). `develop` and `master` are
  level.
- **Python 3.11–3.13** (CI matrix). `mypy` runs under 3.12 semantics (see
  `CLAUDE.md`), strict on `domain/`.
- **~480 unit tests** (+3 network E2E, opt-in) — green; `ruff` and `mypy` clean;
  Sphinx builds with 0 warnings.
- **Seven exchanges**: binance, coinbase, kraken, bybit, okx, bitfinex, bitmex.
- The web UI was reworked (2026-06) into **Data / Historical / Live** pages with
  inline job CRUD; merged via PR #76.

## Repository map

```
dccd/
  domain/         pure, sync, zero-I/O — models, capabilities, transforms, time
  transport/      async I/O primitives — http, ws, ratelimit, paginate
  sources/        one adapter per exchange (+ registry)
  storage/        ParquetStore, RunsStore (SQLite), RemoteStorage
  application/    operations (backfill/stream/read/inventory), Scheduler,
                  EventBus, Config, jobs, registry, monitor, service_factory
  interfaces/     api/ (FastAPI) · cli/ (Typer) · ui/ (Jinja2 templates)
  tests/v3/       all tests
doc/
  source/         Sphinx (end-user docs)
  dev/            this developer brief (+ ui_smoke.py)
deploy/dccd.service  systemd unit ; Dockerfile at root
scripts/          one-off maintenance (e.g. repair_kraken_okx.py)
examples/         config + usage samples
.claude/skills/   data-e2e · release-gate · ui-audit (project skills)
```

> `dccd/{histo_dl,continuous_dl,tools}/` are **empty leftovers** (only stale
> `__pycache__`) from the deleted v2 code — ignore them; the live code is the six
> layers above.

## The data model in one paragraph

Everything internal is **nanosecond UTC `int64`** timestamps. A `Symbol(base,
quote)` is normalised (e.g. `XBT→BTC`) and rendered per exchange. Records are
`OHLCBar`, `Trade`, `OrderBookSnapshot`. Data lands on disk as
`{data_path}/{exchange}/ohlc/{pair}/{span}/YYYY.parquet` (annual) and
`.../trades/{pair}/YYYY-MM-DD.parquet` (daily), de-duplicated per data type.
