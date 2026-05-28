====================================================
 Download Crypto Currencies Data
====================================================

.. div:: hero-banner

   .. raw:: html

      <p class="hero-tagline">
        Crypto market data — REST &amp; WebSocket — 7 exchanges — no API key
      </p>

   |pypi| |python| |license|

   .. grid:: 3
      :gutter: 2

      .. grid-item-card:: Python API
         :link: quickstart
         :link-type: doc
         :class-card: hero-card

         Historical + real-time streams directly in your scripts.

      .. grid-item-card:: CLI Daemon
         :link: daemon
         :link-type: doc
         :class-card: hero-card

         Autonomous collector with YAML config and remote sync.

      .. grid-item-card:: Storage
         :link: storage
         :link-type: doc
         :class-card: hero-card

         Parquet · CSV · SQLite · Polars & Pandas output.

.. |pypi| image:: https://img.shields.io/pypi/v/dccd.svg
   :target: https://pypi.org/project/dccd/
   :alt: PyPI version

.. |python| image:: https://img.shields.io/pypi/pyversions/dccd.svg
   :alt: Python versions

.. |license| image:: https://img.shields.io/github/license/ArthurBernard/Download_Crypto_Currencies_Data.svg
   :alt: License

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
