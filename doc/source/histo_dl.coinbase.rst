Coinbase Historical Downloader (:mod:`dccd.histo_dl.coinbase`)
==============================================================

.. automodule:: dccd.histo_dl.coinbase
   :no-members:
   :no-inherited-members:
   :no-special-members:

:class:`~dccd.histo_dl.coinbase.FromCoinbase` downloads OHLCV candles,
recent trades, and order book snapshots from the Coinbase Exchange public
REST API.  No authentication is required.

Exchange specifics
------------------

.. list-table::
   :widths: 35 65

   * - **Pair format**
     - Hyphen separator: ``'BTC'`` + ``'USD'`` → ``'BTC-USD'``.
   * - **Candles per request**
     - 300 (endpoint: ``GET /products/{pair}/candles``).
   * - **Minimum span**
     - 60 seconds (1 minute).
   * - **Trade history**
     - Recent trades **only** — up to the last 100 trades.  The Coinbase public
       API does not support deep historical pagination.
   * - **Order book depth**
     - Level 2 snapshot (aggregated by price level, no per-order count).
   * - **Authentication**
     - Not required for public data.

Quick example
-------------

.. code-block:: python

   from dccd.histo_dl import FromCoinbase

   obj = FromCoinbase('/data/crypto/', 'BTC', span=3600, fiat='USD')

   # Full OHLCV history (pagination supported)
   obj.import_data(start='2024-01-01 00:00:00', end='2024-12-31 00:00:00')
   obj.save(form='parquet')

   # Only recent trades are available (≤ 100)
   obj.import_trades(start='last', end='now')
   obj.save_trades(form='parquet')
