Bitmex Continuous Downloader (:mod:`dccd.continuous_dl.bitmex`)
===============================================================

.. automodule:: dccd.continuous_dl.bitmex
   :no-members:
   :no-inherited-members:
   :no-special-members:
   :noindex:

:class:`~dccd.continuous_dl.bitmex.DownloadBitmexData` streams real-time
trades and order book data from BitMEX via WebSocket.  No authentication is
required for public channels.

.. note::

   BitMEX does not provide a native OHLCV WebSocket channel via this client.
   Only trade and order book streams are available.

Exchange specifics
------------------

.. list-table::
   :widths: 35 65

   * - **WebSocket URL**
     - ``wss://www.bitmex.com/realtime``
   * - **Available channels**
     - Trades and order book (L2).
   * - **OHLCV**
     - Not available via this client.
   * - **Authentication**
     - Not required for public streams.

Quick example
-------------

.. code-block:: python

   from dccd.continuous_dl import DownloadBitmexData, get_data_bitmex
   from dccd.tools.io import IODataBase

   # Convenience function
   get_data_bitmex('/data/crypto/', pair='XBTUSD',
                   time_step=60, until=3600, form='parquet')

   # Class-based API
   dl = DownloadBitmexData(pair='XBTUSD', time_step=60, until=3600)
   dl.set_trades_saver(IODataBase('/data/crypto/trades', method='parquet'))
   dl.set_book_saver(IODataBase('/data/crypto/book', method='parquet'))
   dl.run()
