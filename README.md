<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/ArthurBernard/Download_Crypto_Currencies_Data/develop/doc/source/_static/logo-dark-transparent.svg">
  <img alt="dccd logo" src="https://raw.githubusercontent.com/ArthurBernard/Download_Crypto_Currencies_Data/develop/doc/source/_static/logo-light-transparent.svg" height="180px" align="left">
</picture>

# **Download Crypto-Currency Data**

[![Python versions](https://img.shields.io/pypi/pyversions/dccd)](https://pypi.org/project/dccd/)
[![PyPI](https://img.shields.io/pypi/v/dccd.svg)](https://pypi.org/project/dccd/)
[![PyPI status](https://img.shields.io/pypi/status/dccd.svg?colorB=blue)](https://pypi.org/project/dccd/)
[![CI](https://github.com/ArthurBernard/Download_Crypto_Currencies_Data/actions/workflows/ci.yml/badge.svg)](https://github.com/ArthurBernard/Download_Crypto_Currencies_Data/actions/workflows/ci.yml)
[![License](https://img.shields.io/github/license/ArthurBernard/Download_Crypto_Currencies_Data.svg)](https://github.com/ArthurBernard/Download_Crypto_Currencies_Data/blob/master/LICENSE.txt)<br>
[![Documentation](https://readthedocs.org/projects/download-crypto-currencies-data/badge/?version=latest)](https://download-crypto-currencies-data.readthedocs.io/en/latest/)
[![Coverage](https://codecov.io/gh/ArthurBernard/Download_Crypto_Currencies_Data/branch/master/graph/badge.svg)](https://codecov.io/gh/ArthurBernard/Download_Crypto_Currencies_Data)
[![Docstring coverage](https://raw.githubusercontent.com/ArthurBernard/Download_Crypto_Currencies_Data/badges/interrogate_badge.svg)](https://github.com/ArthurBernard/Download_Crypto_Currencies_Data)
[![Downloads](https://pepy.tech/badge/dccd)](https://pepy.tech/project/dccd)

___

Python package to download crypto-currency data (OHLCV, trades, order book) from multiple
exchanges via REST and WebSocket APIs. Data can be saved to CSV, Excel, SQLite, PostgreSQL,
or Parquet.

## Installation

```bash
pip install dccd
```

With autonomous daemon support (APScheduler + PyYAML):

```bash
pip install "dccd[daemon]"
```

From source:

```bash
git clone https://github.com/ArthurBernard/Download_Crypto_Currencies_Data
cd Download_Crypto_Currencies_Data
pip install -e .
```

## Supported exchanges

| Exchange | REST OHLCV | REST Trades | REST Order Book | WS OHLCV | WS Trades | WS Order Book |
|----------|:----------:|:-----------:|:---------------:|:--------:|:---------:|:-------------:|
| Binance  | ✓ | ✓ | ✓ | | ✓ | ✓ |
| Coinbase | ✓ | ✓ † | ✓ | | | |
| Kraken   | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Bybit    | ✓ | ✓ † | ✓ | | ✓ | ✓ |
| OKX      | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| Bitfinex | | | | ✓ \* | ✓ | ✓ |
| Bitmex   | | | | | ✓ | ✓ |

\* Bitfinex WS OHLCV is aggregated from the trades stream via `get_ohlc_bitfinex`.  
† Recent trades only (Bybit ≤ 1 000, Coinbase ≤ 100) — no deep historical pagination via the public REST API.

## Presentation

**Historical Downloader** `dccd.histo_dl`  
Download OHLCV data via REST APIs and save to disk. Supports chunked requests, automatic retry on rate-limit (HTTP 429), and incremental updates from the last saved timestamp.

**Continuous Downloader** `dccd.continuous_dl`  
Stream real-time data (order book, trades) via WebSocket with automatic reconnection and configurable processing/saving callbacks.

**Daemon** `dccd.daemon`  
Autonomous, server-side collector driven by a YAML config. Runs REST jobs on a schedule (APScheduler), opens WebSocket streams for real-time collection, and periodically syncs all local data to one or more remote destinations (NAS, S3, SFTP, …) via rclone. Multiple remotes and a configurable sync interval are supported; collection is never blocked by remote availability.

### Output formats

Historical data can be saved as **CSV**, **Excel** (`.xlsx`), **SQLite**, **PostgreSQL** (via SQLAlchemy), or **Parquet**.
All DataFrames are native `polars.DataFrame`. A `pandas.DataFrame` can be obtained via `get_data(format='pandas')`.

## Quick start

Historical data:

```python
from dccd.histo_dl import FromBinance

obj = FromBinance('/path/to/data/', 'BTC', 3600, fiat='USDT')
obj.import_data(start='2024-01-01 00:00:00', end='2024-12-31 00:00:00')
obj.save(form='parquet')
df = obj.get_data()               # polars DataFrame (default)
df_pd = obj.get_data(format='pandas')  # pandas DataFrame (optional)
```

Incremental update (resume from last saved point):

```python
obj.import_data(start='last', end='now').save(form='parquet')
```

Other exchanges:

```python
from dccd.histo_dl import FromKraken, FromBybit, FromOKX

FromKraken('/path/', 'ETH', 3600).import_data(start='2024-01-01', end='now').save()
FromBybit('/path/', 'BTC', 86400).import_data(start='2024-01-01', end='now').save()
FromOKX('/path/', 'BTC', 3600).import_data(start='2024-01-01', end='now').save()
```

Trades (historical or recent):

```python
from dccd.histo_dl import FromBinance, FromKraken

obj = FromBinance('/path/', 'BTC', 3600, fiat='USDT')
obj.import_trades(start='2024-01-01 00:00:00', end='2024-01-02 00:00:00')
obj.save_trades(form='csv')
df = obj.trades_df    # polars DataFrame — columns: TS, price, amount, type, tid

# Kraken also supports full history; Bybit/Coinbase return recent-only snapshots
FromKraken('/path/', 'BTC', 3600).import_trades(start='2024-01-01', end='2024-01-02').save_trades()
```

Order book snapshot:

```python
from dccd.histo_dl import FromOKX

obj = FromOKX('/path/', 'BTC', 3600)
obj.import_orderbook(depth=50)
obj.save_orderbook(form='csv')
df = obj.orderbook_df    # columns: side, price, amount, count
```

Daemon (autonomous collector) — `config.yml`:

```yaml
settings:
  data_path: /data/crypto/
  timezone: UTC

storage:
  remotes:
    - provider: rclone
      remote: "mynas:crypto/"
  sync_interval: 3600

histo_jobs:
  - exchange: binance
    pairs: [BTC/USDT, ETH/USDT]
    span: 3600
    format: parquet

stream_jobs:
  - exchange: binance
    pairs: [BTC/USDT]
    channels: [trades, book]
    time_step: 60
```

CLI quick start:

```bash
# Validate the config
dccd validate --config config.yml

# Backfill all OHLC history defined in config (resumable)
dccd backfill --config config.yml --start "2020-01-01 00:00:00"

# Dry run — estimate windows and time without downloading
dccd backfill --config config.yml --dry-run

# Backfill only one exchange
dccd backfill --config config.yml --exchange kraken

# One incremental batch per job, then exit (for cron)
dccd collect --config config.yml

# Continuous daemon (Ctrl-C to stop)
dccd start --config config.yml

# Add / remove a histo job in-place
dccd add --exchange kraken --pair ETH/USD --span 86400 --config config.yml
dccd remove --exchange kraken --pair ETH/USD --span 86400 --config config.yml

# Inspect all data on disk (OHLC, trades, orderbook)
dccd inventory --config config.yml

# Enable shell tab-completion (run once after install)
dccd --install-completion
```

> `--config` is optional — dccd searches `./config.yml` then `~/.config/dccd/config.yml` when omitted.

Python API:

```python
from dccd.daemon.config import load_config
from dccd.daemon.scheduler import run_once, build_histo_scheduler
from dccd.daemon.stream_manager import StreamManager

config = load_config('config.yml')

# One-shot: download all histo jobs once, then exit
run_once(config)

# Daemon mode: periodic REST + live WebSocket streams
scheduler = build_histo_scheduler(config)
scheduler.start()

mgr = StreamManager(config)
mgr.start()      # runs until mgr.stop() is called
```

## Links

- PyPI: https://pypi.org/project/dccd/
- Documentation: https://download-crypto-currencies-data.readthedocs.io/
- Source: https://github.com/ArthurBernard/Download_Crypto_Currencies_Data
- Changelog: https://github.com/ArthurBernard/Download_Crypto_Currencies_Data/blob/master/CHANGELOG.md
