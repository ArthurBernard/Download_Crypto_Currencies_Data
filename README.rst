=============================
Download Crypto-Currency Data
=============================

.. image:: https://img.shields.io/pypi/pyversions/dccd
    :alt: PyPI - Python Version

.. image:: https://img.shields.io/pypi/v/dccd.svg
    :target: https://pypi.org/project/dccd/
    :alt: PyPI

.. image:: https://img.shields.io/pypi/status/dccd.svg?colorB=blue
    :target: https://pypi.org/project/dccd/
    :alt: PyPI - Status

.. image:: https://github.com/ArthurBernard/Download_Crypto_Currencies_Data/actions/workflows/ci.yml/badge.svg
    :target: https://github.com/ArthurBernard/Download_Crypto_Currencies_Data/actions/workflows/ci.yml
    :alt: CI

.. image:: https://img.shields.io/github/license/ArthurBernard/Download_Crypto_Currencies_Data.svg
    :target: https://github.com/ArthurBernard/Download_Crypto_Currencies_Data/blob/master/LICENSE.txt
    :alt: License

.. image:: https://readthedocs.org/projects/download-crypto-currencies-data/badge/?version=latest
    :target: https://download-crypto-currencies-data.readthedocs.io/en/latest/
    :alt: Documentation Status

.. image:: https://codecov.io/gh/ArthurBernard/Download_Crypto_Currencies_Data/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/ArthurBernard/Download_Crypto_Currencies_Data
    :alt: Coverage

.. image:: https://raw.githubusercontent.com/ArthurBernard/Download_Crypto_Currencies_Data/badges/interrogate_badge.svg
    :target: https://github.com/ArthurBernard/Download_Crypto_Currencies_Data
    :alt: Docstring Coverage

.. image:: https://pepy.tech/badge/dccd
    :target: https://pepy.tech/project/dccd
    :alt: Downloads

Python package to download crypto-currency data (OHLCV, trades, order book) from multiple
exchanges via REST and WebSocket APIs. Data can be saved to CSV, Excel, SQLite, PostgreSQL,
or Parquet.

Installation
============

From pip::

    $ pip install dccd

With optional Parquet / Polars support::

    $ pip install "dccd[io]"

With autonomous daemon support (APScheduler + PyYAML)::

    $ pip install "dccd[daemon]"

From source::

    $ git clone https://github.com/ArthurBernard/Download_Crypto_Currencies_Data
    $ cd Download_Crypto_Currencies_Data
    $ pip install -e .

Supported exchanges
===================

+------------------+------------+-------------+-----------------+----------+-----------+----------------+
| Exchange         | REST OHLCV | REST Trades | REST Order Book | WS OHLCV | WS Trades | WS Order Book  |
+==================+============+=============+=================+==========+===========+================+
| Binance          | ✓          |             |                 |          | ✓         | ✓              |
+------------------+------------+-------------+-----------------+----------+-----------+----------------+
| Coinbase         | ✓          |             |                 |          |           |                |
+------------------+------------+-------------+-----------------+----------+-----------+----------------+
| Kraken           | ✓          |             |                 | ✓        | ✓         | ✓              |
+------------------+------------+-------------+-----------------+----------+-----------+----------------+
| Bybit            | ✓          |             |                 |          | ✓         | ✓              |
+------------------+------------+-------------+-----------------+----------+-----------+----------------+
| OKX              | ✓          |             |                 | ✓        | ✓         | ✓              |
+------------------+------------+-------------+-----------------+----------+-----------+----------------+
| Bitfinex         |            |             |                 | ✓\*      | ✓         | ✓              |
+------------------+------------+-------------+-----------------+----------+-----------+----------------+
| Bitmex           |            |             |                 |          | ✓         | ✓              |
+------------------+------------+-------------+-----------------+----------+-----------+----------------+

\* Bitfinex WS OHLCV is aggregated from the trades stream via ``get_ohlc_bitfinex``.

Presentation
============

**Historical Downloader** ``dccd.histo_dl``
    Download OHLCV data via REST APIs and save to disk. Supports chunked
    requests, automatic retry on rate-limit (HTTP 429), and incremental
    updates from the last saved timestamp.

**Continuous Downloader** ``dccd.continuous_dl``
    Stream real-time data (order book, trades) via WebSocket with automatic
    reconnection and configurable processing/saving callbacks.

**Daemon** ``dccd.daemon``
    Autonomous, server-side collector driven by a YAML config.  Runs REST
    jobs on a schedule (APScheduler), opens WebSocket streams for real-time
    collection, and periodically syncs all local data to one or more remote
    destinations (NAS, S3, SFTP, …) via rclone.  Multiple remotes and a
    configurable sync interval are supported; collection is never blocked by
    remote availability.

Output formats
--------------

Historical data can be saved as **CSV**, **Excel** (``.xlsx``), **SQLite**,
**PostgreSQL** (via SQLAlchemy), or **Parquet** (requires ``dccd[io]``).
Parquet files can be read back as either a ``pandas.DataFrame`` or a
``polars.DataFrame``.

Quick start
===========

Historical data (pandas)::

    from dccd.histo_dl import FromBinance

    obj = FromBinance('/path/to/data/', 'BTC', 3600, fiat='USDT')
    obj.import_data(start='2024-01-01 00:00:00', end='2024-12-31 00:00:00')
    obj.save(form='parquet')
    df = obj.get_data()            # pandas DataFrame

Polars output::

    df_pl = obj.get_data(format='polars')

Incremental update (resume from last saved point)::

    obj.import_data(start='last', end='now').save(form='parquet')

Other exchanges::

    from dccd.histo_dl import FromKraken, FromBybit, FromOKX

    FromKraken('/path/', 'ETH', 3600).import_data(start='2024-01-01', end='now').save()
    FromBybit('/path/', 'BTC', 86400).import_data(start='2024-01-01', end='now').save()
    FromOKX('/path/', 'BTC', 3600).import_data(start='2024-01-01', end='now').save()

Daemon (autonomous collector)::

    # config.yml
    # storage:
    #   local_path: /data/crypto/
    #   remotes:
    #     - provider: rclone
    #       remote: "mynas:crypto/"
    #   sync_interval: 3600
    # histo_jobs:
    #   - exchange: binance
    #     pairs: [BTC/USDT, ETH/USDT]
    #     span: 3600
    #     format: parquet
    #     by_period: Y
    # stream_jobs:
    #   - exchange: binance
    #     pairs: [BTC/USDT]
    #     channels: [trades, book]
    #     time_step: 60

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
    mgr.start()      # blocks until mgr.stop() is called

Links
=====

- PyPI: https://pypi.org/project/dccd/
- Documentation: https://download-crypto-currencies-data.readthedocs.io/
- Source: https://github.com/ArthurBernard/Download_Crypto_Currencies_Data
- Changelog: https://github.com/ArthurBernard/Download_Crypto_Currencies_Data/blob/master/CHANGELOG.md
