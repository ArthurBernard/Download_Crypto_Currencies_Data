<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/ArthurBernard/Download_Crypto_Currencies_Data/develop/doc/source/_static/logo-dark-transparent.svg">
  <img alt="dccd logo" src="https://raw.githubusercontent.com/ArthurBernard/Download_Crypto_Currencies_Data/develop/doc/source/_static/logo-light-transparent.svg" height="180px" align="left">
</picture>

# **Download Crypto-Currency Data** — v3

[![Python versions](https://img.shields.io/pypi/pyversions/dccd)](https://pypi.org/project/dccd/)
[![PyPI](https://img.shields.io/pypi/v/dccd.svg)](https://pypi.org/project/dccd/)
[![CI](https://github.com/ArthurBernard/Download_Crypto_Currencies_Data/actions/workflows/ci.yml/badge.svg)](https://github.com/ArthurBernard/Download_Crypto_Currencies_Data/actions/workflows/ci.yml)
[![License](https://img.shields.io/github/license/ArthurBernard/Download_Crypto_Currencies_Data.svg)](https://github.com/ArthurBernard/Download_Crypto_Currencies_Data/blob/master/LICENSE.txt)<br>
[![Documentation](https://readthedocs.org/projects/download-crypto-currencies-data/badge/?version=latest)](https://download-crypto-currencies-data.readthedocs.io/en/latest/)
[![Coverage](https://codecov.io/gh/ArthurBernard/Download_Crypto_Currencies_Data/branch/master/graph/badge.svg)](https://codecov.io/gh/ArthurBernard/Download_Crypto_Currencies_Data)

---

**dccd** downloads crypto-currency market data (OHLCV, trades, order book)
from 7 exchanges via REST and WebSocket. Data is stored as Parquet files with
nanosecond-precision timestamps.

## Architecture (v3)

Hexagonal architecture — business logic is fully separated from interfaces:

```
Interfaces: CLI · HTTP API · Web UI · Python Client
                        ↓
          Application: backfill, stream, read, inventory
                        ↓
  Domain ← Sources (7 exchange adapters) ← Transport (httpx · WS · Paginator)
                        ↓
             Storage: ParquetStore + RunsStore (SQLite)
```

- **Async-first** — httpx + websockets, one event loop; CLI via `asyncio.run`
- **Nanosecond timestamps** — uniform int64 UTC throughout the store
- **Generic Paginator** — no per-exchange chunking; Coinbase 300-limit is a capability declaration
- **NoCapability early** — Bybit no spot trades history, Kraken OHLC recent-only → clear error
- **Four iso-functional interfaces** — same operations everywhere (parity test enforces this)

## Supported exchanges

You pick a **data type** (OHLC · trades · order book) and an **operation** —
**backfill** (history) or **stream** (live):

| Exchange | Backfill (history) | Stream (live) |
|----------|--------------------|---------------|
| Binance  | OHLC · trades · book | OHLC · trades · book |
| Coinbase | OHLC · book · trades *(recent)* | trades |
| Kraken   | OHLC *(720 recent)* · trades · book | OHLC · trades · book |
| Bybit    | OHLC · book | OHLC · trades · book |
| OKX      | OHLC · trades · book | OHLC · trades · book |
| Bitfinex | OHLC · trades · book | OHLC · trades |
| BitMEX   | OHLC *(1m/5m/1h/1d)* · trades · book | OHLC · trades · book |

Trades backfill is **cursor-paginated** (drains the full window, not just the
first page). *recent* = no deep history via the public API (a deeper request is
rejected/clamped early, never silently truncated); Bybit spot has no trade
history. **Order-book backfill** is a single snapshot — use a stream to record
the book over time. Stream channels are only listed where really implemented
(undeclared ones raise `NoCapability`).

### OHLC field fidelity

Not every exchange returns every OHLC field natively. Missing fields are stored
as `null` (never fabricated):

| Exchange | `quote_volume` | `trades` (count) |
|----------|----------------|------------------|
| Binance  | ✅ native      | ✅ native |
| Bybit / OKX | ✅ native   | — null |
| Kraken   | ✅ (vwap × volume, exact) | ✅ native |
| Coinbase / Bitfinex / BitMEX | — null | — null |

## Installation

```bash
# Core — Python 3.11+
pip install dccd

# With scheduler, CLI, and web UI
pip install "dccd[daemon]"

# Development
pip install "dccd[dev]"
```

## Quick start

### Python API

```python
import asyncio
from dccd import Client

async def main():
    async with Client() as c:
        result = await c.backfill("binance", "BTC/USDT", data_type="ohlc", span=3600)
        print(f"Wrote {result['rows_written']} rows")
        for ds in c.inventory():
            print(ds)

asyncio.run(main())
```

### CLI

```bash
dccd validate --config config.yml      # validate config
dccd backfill --config config.yml      # run all backfill jobs
dccd backfill -e binance -s BTC/USDT --type ohlc --span 3600  # ad-hoc
dccd stream   --config config.yml      # run WebSocket stream jobs
dccd start    --config config.yml      # full daemon + UI
dccd ui       --config config.yml      # UI only (no scheduler)
dccd inventory --config config.yml     # list stored datasets
dccd status   --config config.yml      # show recent runs
```

### Configuration (`config.yml`)

```yaml
settings:
  data_path: ./data/crypto
  timezone: UTC
  ui_port: 8080

jobs:
  - exchange: binance
    pairs: [BTC/USDT, ETH/USDT]
    data_type: ohlc
    span: 3600
    trigger_kind: interval
    every: 3600

  - exchange: kraken
    pairs: [BTC/USD]
    data_type: trades
    operation: stream
    trigger_kind: supervised

storage:
  remotes:
    - provider: rclone
      remote: "mynas:crypto/"
  sync_interval: 3600
```

### HTTP API (when `dccd ui` or `dccd start` is running)

```
GET  /api/operations          list registered operations
POST /api/backfill            start a backfill job
GET  /api/backfill/{run_id}   poll run status
GET  /api/streams             list stream jobs + state
POST /api/streams/start       start a stream job
POST /api/streams/stop        stop a stream job
POST /api/read                read stored data (≤1 000 rows)
GET  /api/events              SSE stream of progress/log/status events
GET  /api/inventory           list all datasets
GET  /health                  liveness check
```

## Data layout

```
{data_path}/
  {exchange}/
    ohlc/{pair}/{span}/YYYY.parquet       # annual, ns timestamps
    trades/{pair}/YYYY-MM-DD.parquet      # daily
    orderbook/{pair}/YYYY-MM-DD.parquet   # daily
    .dccd/runs.db                         # SQLite job run history
```

All timestamps are **nanoseconds UTC** (int64).

## Development

```bash
pip install -e ".[dev]"
pytest             # 141 tests
ruff check dccd/   # lint
mypy dccd/         # type check (strict on domain/)
```
