Binance Historical Downloader (:mod:`dccd.histo_dl.binance`)
============================================================

.. automodule:: dccd.histo_dl.binance
   :no-members:
   :no-inherited-members:
   :no-special-members:

:class:`~dccd.histo_dl.binance.FromBinance` downloads OHLCV candles,
trade history, and order book snapshots from the Binance public REST API.
No authentication is required.

Exchange specifics
------------------

.. list-table::
   :widths: 35 65

   * - **Pair format**
     - Concatenation without separator: ``'BTC'`` + ``'USDT'`` → ``'BTCUSDT'``.
       If *fiat* is ``'EUR'`` or ``'USD'`` it is silently coerced to ``'USDT'``
       (Binance does not support fiat; only USDT is accepted).
   * - **Candles per request**
     - 1 000 (endpoint: ``GET /api/v3/klines``).
   * - **Minimum span**
     - 60 seconds (1 minute).
   * - **Trade history**
     - Full historical depth via ``GET /api/v3/aggTrades`` (paginated by trade
       ID).
   * - **Order book depth**
     - Snapshot at depth 5, 10, 20, 50, 100, 500, 1 000, or 5 000 price
       levels per side.
   * - **Authentication**
     - Not required for public data.

Quick example
-------------

.. code-block:: python

   from dccd.histo_dl import FromBinance

   obj = FromBinance('/data/crypto/', 'BTC', span=3600, fiat='USDT')

   # Download 2024 hourly candles
   obj.import_data(start='2024-01-01 00:00:00', end='2024-12-31 00:00:00')
   obj.save(form='parquet')

   # Incremental update
   obj.import_data(start='last', end='now').save(form='parquet')

   # Trade history
   obj.import_trades(start='2024-01-01 00:00:00', end='2024-03-31 00:00:00')
   obj.save_trades(form='parquet')

   # Order book snapshot (20 levels per side)
   obj.import_orderbook(depth=20)
   obj.save_orderbook(form='parquet')
