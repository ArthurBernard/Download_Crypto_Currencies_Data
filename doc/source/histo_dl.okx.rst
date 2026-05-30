OKX Historical Downloader (:mod:`dccd.histo_dl.okx`)
=====================================================

.. automodule:: dccd.histo_dl.okx
   :no-members:
   :no-inherited-members:
   :no-special-members:

:class:`~dccd.histo_dl.okx.FromOKX` downloads OHLCV candles,
trade history, and order book snapshots from the OKX public REST API.
No authentication is required.

Exchange specifics
------------------

.. list-table::
   :widths: 35 65

   * - **Pair format**
     - Hyphen separator with *instrument ID* format:
       ``'BTC'`` + ``'USDT'`` → ``'BTC-USDT'``.
   * - **OHLCV endpoint**
     - Uses ``GET /api/v5/market/history-candles`` — **not** ``/market/candles``
       which only returns the last ~24 h of 1-minute bars.
   * - **Candles per request**
     - 300.
   * - **Minimum span**
     - 60 seconds (1 minute).
   * - **Trade history**
     - Full historical depth via ``GET /api/v5/market/trades``.
   * - **Order book depth**
     - Snapshot at 1, 5, 400, or 4 000 levels per side.
   * - **Authentication**
     - Not required for public data.

Quick example
-------------

.. code-block:: python

   from dccd.histo_dl import FromOKX

   obj = FromOKX('/data/crypto/', 'BTC', span=3600, fiat='USDT')

   # Full OHLCV history
   obj.import_data(start='2024-01-01 00:00:00', end='2024-12-31 00:00:00')
   obj.save(form='parquet')

   # Trade history (full pagination)
   obj.import_trades(start='2024-01-01 00:00:00', end='2024-06-30 00:00:00')
   obj.save_trades(form='parquet')
