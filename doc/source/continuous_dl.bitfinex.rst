Bitfinex Continuous Downloader (:mod:`dccd.continuous_dl.bitfinex`)
===================================================================

.. automodule:: dccd.continuous_dl.bitfinex
   :no-members:
   :no-inherited-members:
   :no-special-members:
   :noindex:

:class:`~dccd.continuous_dl.bitfinex.DownloadBitfinexData` streams real-time
trades and order book data from Bitfinex via WebSocket.  No authentication is
required.

.. note::

   Bitfinex does **not** provide a native OHLCV WebSocket channel.  OHLCV bars
   are instead computed from the trade stream by
   :func:`~dccd.continuous_dl.bitfinex.get_ohlc_bitfinex`, which aggregates
   trades into candles at the requested ``time_step``.

Exchange specifics
------------------

.. list-table::
   :widths: 35 65

   * - **WebSocket URL**
     - ``wss://api-pub.bitfinex.com/ws/2``
   * - **Available channels**
     - Trades (``trades``) and order book (``book``).
   * - **OHLCV**
     - Derived from the trades stream via aggregation (not a native channel).
   * - **Authentication**
     - Not required for public streams.

Quick example
-------------

.. code-block:: python

   from dccd.continuous_dl import DownloadBitfinexData, get_data_bitfinex
   from dccd.tools.io import IODataBase

   # Convenience function
   get_data_bitfinex('/data/crypto/', pair='tBTCUSD',
                     time_step=60, until=3600, form='parquet')

   # Class-based API
   dl = DownloadBitfinexData(pair='tBTCUSD', time_step=60, until=3600)
   dl.set_trades_saver(IODataBase('/data/crypto/trades', method='parquet'))
   dl.set_book_saver(IODataBase('/data/crypto/book', method='parquet'))
   dl.run()
