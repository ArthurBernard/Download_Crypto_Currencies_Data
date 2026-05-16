====================================================
 Welcome to Download Crypto Currencies Data project
====================================================

This is the documentation of ``dccd`` package, a package to download crypto-currencies data from multiple exchanges via REST and WebSocket APIs.

Installation
------------

From pip:

.. code-block:: bash

   pip install dccd

From source:

.. code-block:: bash

   git clone https://github.com/ArthurBernard/Download_Crypto_Currencies_Data.git
   cd Download_Crypto_Currencies_Data
   pip install -e .

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
     -
     -
     -
     - ✓
     - ✓
   * - Coinbase
     - ✓
     -
     -
     -
     -
     -
   * - Kraken
     - ✓
     -
     -
     - ✓
     - ✓
     - ✓
   * - Bybit
     - ✓
     -
     -
     -
     - ✓
     - ✓
   * - OKX
     - ✓
     -
     -
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

Presentation
------------

The ``dccd`` package provides three ways to download data:

- **Historical Downloader** :mod:`dccd.histo_dl`:
   Download OHLCV data via REST APIs with chunked requests and incremental updates.
   Supports Binance, Coinbase, Kraken, Bybit, and OKX.
- **Continuous Downloader** :mod:`dccd.continuous_dl`:
   Stream real-time data (order book, trades, OHLCV) via WebSocket with automatic
   reconnection. Supports Binance, Bitfinex, Bitmex, Bybit, Kraken, and OKX.
- **Daemon** :mod:`dccd.daemon`:
   Autonomous, server-side collector driven by a YAML config.  Runs REST jobs on a
   schedule (APScheduler), opens WebSocket streams, and syncs data to remote
   destinations (NAS, S3, SFTP, …) via rclone.

Contents
--------

.. toctree::
   :maxdepth: 2

   continuous_dl
   continuous_dl.binance
   continuous_dl.bitfinex
   continuous_dl.bitmex
   continuous_dl.bybit
   continuous_dl.kraken
   continuous_dl.okx
   histo_dl
   histo_dl.binance
   histo_dl.coinbase
   histo_dl.kraken
   histo_dl.bybit
   histo_dl.okx
   daemon
   process_data
   tools
