Binance Continuous Downloader (:mod:`dccd.continuous_dl.binance`)
=================================================================

.. automodule:: dccd.continuous_dl.binance
   :noindex:
   :no-members:
   :no-inherited-members:
   :no-special-members:

:class:`~dccd.continuous_dl.binance.DownloadBinanceData` streams real-time
trades and order book data from Binance via WebSocket.  No authentication is
required.

Exchange specifics
------------------

.. list-table::
   :widths: 35 65

   * - **WebSocket URL**
     - ``wss://stream.binance.com:9443/stream``
   * - **Available channels**
     - Trades (``<symbol>@aggTrade``) and order book diff (``<symbol>@depth``).
   * - **Pair format**
     - Lowercase concatenation: ``'btcusdt'`` (handled automatically).
   * - **Authentication**
     - Not required for public streams.

Quick example
-------------

.. code-block:: python

   from dccd.continuous_dl import DownloadBinanceData, get_data_binance
   from dccd.tools.io import IODataBase

   # Convenience function (simplest)
   get_data_binance('/data/crypto/', pair='BTCUSDT',
                    time_step=60, until=3600, form='parquet')

   # Class-based API (full control)
   dl = DownloadBinanceData(pair='BTCUSDT', time_step=60, until=3600)
   dl.set_trades_saver(IODataBase('/data/crypto/trades', method='parquet'))
   dl.set_book_saver(IODataBase('/data/crypto/book', method='parquet'))
   dl.run()
