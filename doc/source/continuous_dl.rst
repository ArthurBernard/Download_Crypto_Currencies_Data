---------------------------------------------------
 Continuous Downloader (:mod:`dccd.continuous_dl`)
---------------------------------------------------

.. automodule:: dccd.continuous_dl
   :no-members:
   :no-inherited-members:
   :no-special-members:

Streams real-time data (trades, order book, OHLCV) via WebSocket with
automatic reconnection.  Each exchange provides a downloader class and
convenience functions.  The ``time_step`` parameter controls how often data
is snapshotted to disk (in seconds); ``until`` sets the total run duration
(in seconds, or as an absolute timestamp).

Convenience functions vs. class API
-------------------------------------

.. list-table::
   :header-rows: 1
   :widths: 35 30 35

   * -
     - Convenience functions
     - Downloader class
   * - **Configuration**
     - Fixed parameters
     - Custom callbacks via ``set_process_data()``
   * - **Save format**
     - Default CSV or Parquet
     - Any :class:`~dccd.tools.io.IODataBase` saver
   * - **Typical use**
     - Quick one-shot script
     - Embedded in a long-running service

**Convenience function** (simplest):

.. code-block:: python

   from dccd.continuous_dl import get_data_binance

   # Stream BTC/USDT trades + book, save every 60 s for 1 h
   get_data_binance('/data/crypto/', pair='BTCUSDT',
                    time_step=60, until=3600, form='parquet')

**Class-based API** (full control):

.. code-block:: python

   from dccd.continuous_dl import DownloadBinanceData
   from dccd.tools.io import IODataBase

   dl = DownloadBinanceData(pair='BTCUSDT', time_step=60, until=3600)
   dl.set_trades_saver(IODataBase('/data/crypto/trades', method='parquet'))
   dl.set_book_saver(IODataBase('/data/crypto/book', method='parquet'))
   dl.run()

For exchange-specific details (WebSocket URL, available channels, pair
format) see the exchange pages in the sidebar.

Downloader classes
------------------

.. autosummary::
   :toctree: generated/

   binance.DownloadBinanceData -- stream order book and trades from Binance
   bitfinex.DownloadBitfinexData -- stream order book and trades from Bitfinex
   bitmex.DownloadBitmexData -- stream order book and trades from Bitmex
   bybit.DownloadBybitData -- stream order book and trades from Bybit
   kraken.DownloadKrakenData -- stream order book and trades from Kraken
   okx.DownloadOKXData -- stream order book and trades from OKX

Convenience functions
---------------------

.. autosummary::
   :toctree: generated/

   binance.get_data_binance -- download data from Binance exchange and update the database
   binance.get_orderbook_binance -- download order book from Binance exchange and update the database
   binance.get_trades_binance -- download trades from Binance exchange and update the database
   bitfinex.get_data_bitfinex -- download data from Bitfinex exchange and update the database
   bitfinex.get_orderbook_bitfinex -- download order book from Bitfinex exchange and update the database
   bitfinex.get_trades_bitfinex -- download trades from Bitfinex exchange and update the database
   bitmex.get_data_bitmex -- download data from Bitmex exchange and update the database
   bitmex.get_orderbook_bitmex -- download order book from Bitmex exchange and update the database
   bitmex.get_trades_bitmex -- download trades from Bitmex exchange and update the database
   bybit.get_data_bybit -- download data from Bybit exchange and update the database
   bybit.get_orderbook_bybit -- download order book from Bybit exchange and update the database
   bybit.get_trades_bybit -- download trades from Bybit exchange and update the database
   kraken.get_data_kraken -- download data from Kraken exchange and update the database
   kraken.get_orderbook_kraken -- download order book from Kraken exchange and update the database
   kraken.get_trades_kraken -- download trades from Kraken exchange and update the database
   okx.get_data_okx -- download data from OKX exchange and update the database
   okx.get_orderbook_okx -- download order book from OKX exchange and update the database
   okx.get_trades_okx -- download trades from OKX exchange and update the database
