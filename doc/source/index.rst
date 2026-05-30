.. raw:: html

   <div class="hero-header">
     <img class="only-light" src="_static/logo-light-transparent.svg" alt="dccd logo">
     <img class="only-dark"  src="_static/logo-dark-transparent.svg"  alt="dccd logo">
     <h1 class="hero-title">Download Crypto Currencies Data</h1>
   </div>

.. rst-class:: hidden-rst-title

====================================================
 Download Crypto Currencies Data
====================================================

|python| |pypi| |status| |ci| |license| |docs| |coverage| |interrogate| |downloads|

.. |python| image:: https://img.shields.io/pypi/pyversions/dccd.svg
   :alt: Python versions

.. |pypi| image:: https://img.shields.io/pypi/v/dccd.svg
   :target: https://pypi.org/project/dccd/
   :alt: PyPI version

.. |status| image:: https://img.shields.io/pypi/status/dccd.svg?colorB=blue
   :target: https://pypi.org/project/dccd/
   :alt: PyPI status

.. |ci| image:: https://github.com/ArthurBernard/Download_Crypto_Currencies_Data/actions/workflows/ci.yml/badge.svg
   :target: https://github.com/ArthurBernard/Download_Crypto_Currencies_Data/actions/workflows/ci.yml
   :alt: CI

.. |license| image:: https://img.shields.io/github/license/ArthurBernard/Download_Crypto_Currencies_Data.svg
   :target: https://github.com/ArthurBernard/Download_Crypto_Currencies_Data/blob/master/LICENSE.txt
   :alt: License

.. |docs| image:: https://readthedocs.org/projects/download-crypto-currencies-data/badge/?version=latest
   :target: https://download-crypto-currencies-data.readthedocs.io/en/latest/
   :alt: Documentation Status

.. |coverage| image:: https://codecov.io/gh/ArthurBernard/Download_Crypto_Currencies_Data/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/ArthurBernard/Download_Crypto_Currencies_Data
   :alt: Coverage

.. |interrogate| image:: https://raw.githubusercontent.com/ArthurBernard/Download_Crypto_Currencies_Data/badges/interrogate_badge.svg
   :target: https://github.com/ArthurBernard/Download_Crypto_Currencies_Data
   :alt: Docstring coverage

.. |downloads| image:: https://pepy.tech/badge/dccd
   :target: https://pepy.tech/project/dccd
   :alt: Downloads

``dccd`` downloads crypto-currency data (OHLCV, trades, order book) from
multiple exchanges via REST and WebSocket APIs.

.. code-block:: bash

   pip install dccd

.. grid:: 3
   :gutter: 3

   .. grid-item-card:: Python API
      :link: quickstart
      :link-type: doc

      Historical REST downloads and real-time WebSocket streams — use
      ``dccd`` directly in your scripts or notebooks.

   .. grid-item-card:: CLI Daemon
      :link: daemon
      :link-type: doc

      Autonomous server-side collector driven by a YAML config with
      scheduling, WebSocket streams, and rclone remote sync.

   .. grid-item-card:: Storage & Formats
      :link: storage
      :link-type: doc

      Annual Parquet files by default · CSV · Excel · SQLite ·
      PostgreSQL · Polars & Pandas output.

.. rubric:: Key features

- **7 exchanges** — Binance, Coinbase, Kraken, Bybit, OKX, Bitfinex, Bitmex
- **3 data types** — OHLCV candles, trade history, order book snapshots
- **Incremental updates** — ``start='last'`` resumes from the last saved timestamp, no duplicates
- **Polars-native output** — ``get_data()`` returns a :class:`polars.DataFrame`; ``get_data(format='pandas')`` for Pandas
- **No API key required** — all endpoints used are public
- **Autonomous daemon** — YAML config, APScheduler, WebSocket streams, rclone remote sync

.. _supported-exchanges:

Supported exchanges
-------------------

.. list-table::
   :header-rows: 1
   :stub-columns: 1

   * - Exchange
     - REST OHLCV
     - REST Trades
     - REST Order Book
     - WS OHLCV
     - WS Trades
     - WS Order Book
   * - Binance
     - ✓
     - ✓
     - ✓
     -
     - ✓
     - ✓
   * - Coinbase
     - ✓
     - ✓ †
     - ✓
     -
     -
     -
   * - Kraken
     - ✓
     - ✓
     - ✓
     - ✓
     - ✓
     - ✓
   * - Bybit
     - ✓
     - ✓ †
     - ✓
     -
     - ✓
     - ✓
   * - OKX
     - ✓
     - ✓
     - ✓
     - ✓
     - ✓
     - ✓
   * - Bitfinex
     -
     -
     -
     - ✓ \*
     - ✓
     - ✓
   * - Bitmex
     -
     -
     -
     -
     - ✓
     - ✓

\* Bitfinex WS OHLCV is aggregated from the trades stream via :func:`~dccd.continuous_dl.bitfinex.get_ohlc_bitfinex`.

† Recent trades only (Bybit ≤ 1 000, Coinbase ≤ 100) — no deep historical pagination via the public REST API.

.. toctree::
   :hidden:
   :caption: Getting Started

   installation
   quickstart
   changelog

.. toctree::
   :hidden:
   :caption: Data Collection

   histo_dl
   continuous_dl
   daemon

.. toctree::
   :hidden:
   :caption: Reference

   storage
   models
   tools
   process_data
   cli
   configuration
