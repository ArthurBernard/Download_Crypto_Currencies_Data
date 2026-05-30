Kraken Continuous Downloader (:mod:`dccd.continuous_dl.kraken`)
================================================================

.. automodule:: dccd.continuous_dl.kraken
   :noindex:
   :no-members:
   :no-inherited-members:
   :no-special-members:

:class:`~dccd.continuous_dl.kraken.DownloadKrakenData` streams real-time
trades, order book data, and native OHLCV candles from Kraken via WebSocket.
Kraken is the only exchange supported by ``dccd`` that provides native OHLCV
over WebSocket.  No authentication is required.

Exchange specifics
------------------

.. list-table::
   :widths: 35 65

   * - **WebSocket URL**
     - ``wss://ws.kraken.com/v2``
   * - **Available channels**
     - Trades, order book (level 2 with depth 10–1000), and OHLCV (native).
   * - **Pair format**
     - Standard slash-separated: ``'BTC/USD'`` (handled automatically).
   * - **Authentication**
     - Not required for public streams.

Quick example
-------------

.. code-block:: python

   from dccd.continuous_dl import DownloadKrakenData, get_data_kraken
   from dccd.tools.io import IODataBase

   # Convenience function
   get_data_kraken('/data/crypto/', pair='BTC/USD',
                   time_step=60, until=3600, form='parquet')

   # Class-based API
   dl = DownloadKrakenData(pair='BTC/USD', time_step=60, until=3600)
   dl.set_trades_saver(IODataBase('/data/crypto/trades', method='parquet'))
   dl.set_book_saver(IODataBase('/data/crypto/book', method='parquet'))
   dl.run()
