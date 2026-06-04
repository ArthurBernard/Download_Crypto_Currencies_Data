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

.. raw:: html

   <div class="badge-row">
     <img src="https://img.shields.io/pypi/pyversions/dccd.svg" alt="Python versions">
     <a href="https://pypi.org/project/dccd/"><img src="https://img.shields.io/pypi/v/dccd.svg" alt="PyPI version"></a>
     <a href="https://pypi.org/project/dccd/"><img src="https://img.shields.io/pypi/status/dccd.svg?colorB=blue" alt="PyPI status"></a>
     <a href="https://github.com/ArthurBernard/Download_Crypto_Currencies_Data/actions/workflows/ci.yml"><img src="https://github.com/ArthurBernard/Download_Crypto_Currencies_Data/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
     <a href="https://github.com/ArthurBernard/Download_Crypto_Currencies_Data/blob/master/LICENSE.txt"><img src="https://img.shields.io/github/license/ArthurBernard/Download_Crypto_Currencies_Data.svg" alt="License"></a>
     <a href="https://download-crypto-currencies-data.readthedocs.io/en/latest/"><img src="https://readthedocs.org/projects/download-crypto-currencies-data/badge/?version=latest" alt="Documentation"></a>
     <a href="https://codecov.io/gh/ArthurBernard/Download_Crypto_Currencies_Data"><img src="https://codecov.io/gh/ArthurBernard/Download_Crypto_Currencies_Data/branch/master/graph/badge.svg" alt="Coverage"></a>
     <a href="https://github.com/ArthurBernard/Download_Crypto_Currencies_Data"><img src="https://raw.githubusercontent.com/ArthurBernard/Download_Crypto_Currencies_Data/badges/interrogate_badge.svg" alt="Docstring coverage"></a>
     <a href="https://pepy.tech/project/dccd"><img src="https://pepy.tech/badge/dccd" alt="Downloads"></a>
   </div>

``dccd`` downloads crypto-currency data (OHLCV, trades, order book) from
multiple exchanges via REST and WebSocket APIs.

.. code-block:: bash

   pip install dccd

.. grid:: 1 1 2 3
   :gutter: 3

   .. grid-item-card:: Python API
      :link: quickstart
      :link-type: doc

      Historical REST downloads and real-time WebSocket streams — use
      ``dccd`` directly in your scripts or notebooks.

   .. grid-item-card:: CLI & Daemon
      :link: cli
      :link-type: doc

      ``dccd`` command line: ``backfill``, ``stream``, ``start`` (daemon +
      web UI), ``migrate``, ``inventory`` — driven by a YAML config.

   .. grid-item-card:: API Reference
      :link: api
      :link-type: doc

      The hexagonal layers — domain, transport, sources, storage,
      application, interfaces.

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

\* Bitfinex WS OHLCV is delivered natively on the ``candles`` channel.

† Recent trades only (Coinbase) — no deep historical pagination via the public
REST API; a deep request is rejected early rather than silently truncated.
Bybit spot has no trade history at all (WS only). All other trade backfills are
cursor-paginated and drain the full requested window.

.. toctree::
   :hidden:
   :caption: Getting Started

   installation
   quickstart
   changelog

.. toctree::
   :hidden:
   :caption: Reference

   cli
   configuration
   api
