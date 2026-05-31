Bybit Continuous Downloader (:mod:`dccd.continuous_dl.bybit`)
==============================================================

.. automodule:: dccd.continuous_dl.bybit
   :noindex:
   :no-members:
   :no-inherited-members:
   :no-special-members:

:class:`~dccd.continuous_dl.bybit.DownloadBybitData` streams real-time
trades and order book data from Bybit via WebSocket (v5 public API).
No authentication is required.

Exchange specifics
------------------

.. list-table::
   :widths: 35 65

   * - **WebSocket URL**
     - ``wss://stream.bybit.com/v5/public/spot``
   * - **Available channels**
     - Trades (``publicTrade``) and order book (``orderbook``).
   * - **Pair format**
     - Concatenation: ``'BTCUSDT'`` (handled automatically).
   * - **Authentication**
     - Not required for public streams.

Quick example
-------------

.. code-block:: python

   from dccd.continuous_dl import DownloadBybitData, get_data_bybit
   from dccd.tools.io import IODataBase

   # Convenience function
   get_data_bybit('/data/crypto/', pair='BTCUSDT',
                  time_step=60, until=3600, form='parquet')

   # Class-based API
   dl = DownloadBybitData(pair='BTCUSDT', time_step=60, until=3600)
   dl.set_trades_saver(IODataBase('/data/crypto/trades', method='parquet'))
   dl.set_book_saver(IODataBase('/data/crypto/book', method='parquet'))
   dl.run()
