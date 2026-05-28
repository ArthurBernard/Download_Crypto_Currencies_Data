====================================================
 Download Crypto Currencies Data
====================================================

``dccd`` downloads crypto-currency data (OHLCV, trades, order book) from
multiple exchanges via REST and WebSocket APIs.  Choose the mode that fits
your workflow:

.. grid:: 3
   :gutter: 3

   .. grid-item-card:: Python API
      :link: getting_started
      :link-type: doc

      Historical REST downloads and real-time WebSocket streams — use
      ``dccd`` directly in your scripts or notebooks.

   .. grid-item-card:: CLI Daemon
      :link: daemon
      :link-type: doc

      Autonomous server-side collector driven by a YAML config with
      scheduling, WebSocket streams, and rclone remote sync.

   .. grid-item-card:: Supported Exchanges
      :link: #supported-exchanges
      :link-type: url

      Binance, Coinbase, Kraken, Bybit, OKX (REST + WS) · Bitfinex,
      Bitmex (WS only).

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

   getting_started

.. toctree::
   :hidden:
   :caption: Historical Downloader

   histo_dl
   histo_dl.binance
   histo_dl.coinbase
   histo_dl.kraken
   histo_dl.bybit
   histo_dl.okx

.. toctree::
   :hidden:
   :caption: Continuous Downloader

   continuous_dl
   continuous_dl.binance
   continuous_dl.bitfinex
   continuous_dl.bitmex
   continuous_dl.bybit
   continuous_dl.kraken
   continuous_dl.okx

.. toctree::
   :hidden:
   :caption: Daemon

   daemon

.. toctree::
   :hidden:
   :caption: Reference

   storage
   models
   tools
   tools.date_time
   tools.io
   tools.websocket
   process_data
