OKX Continuous Downloader (:mod:`dccd.continuous_dl.okx`)
==========================================================

.. automodule:: dccd.continuous_dl.okx
   :noindex:
   :no-members:
   :no-inherited-members:
   :no-special-members:

:class:`~dccd.continuous_dl.okx.DownloadOKXData` streams real-time trades
and order book data from OKX via WebSocket.  No authentication is required.

Exchange specifics
------------------

.. list-table::
   :widths: 35 65

   * - **WebSocket URL**
     - ``wss://ws.okx.com:8443/ws/v5/public``
   * - **Available channels**
     - Trades (``trades``) and order book (``books`` / ``books5``).
   * - **Pair format**
     - Hyphen-separated instrument ID: ``'BTC-USDT'`` (handled automatically).
   * - **Authentication**
     - Not required for public streams.

Quick example
-------------

.. code-block:: python

   from dccd.continuous_dl import DownloadOKXData, get_data_okx
   from dccd.tools.io import IODataBase

   # Convenience function
   get_data_okx('/data/crypto/', pair='BTC-USDT',
                time_step=60, until=3600, form='parquet')

   # Class-based API
   dl = DownloadOKXData(pair='BTC-USDT', time_step=60, until=3600)
   dl.set_trades_saver(IODataBase('/data/crypto/trades', method='parquet'))
   dl.set_book_saver(IODataBase('/data/crypto/book', method='parquet'))
   dl.run()
