Bybit Historical Downloader (:mod:`dccd.histo_dl.bybit`)
=========================================================

.. automodule:: dccd.histo_dl.bybit
   :no-members:
   :no-inherited-members:
   :no-special-members:

:class:`~dccd.histo_dl.bybit.FromBybit` downloads OHLCV candles,
recent trades, and order book snapshots from the Bybit v5 public REST API.
No authentication is required.

Exchange specifics
------------------

.. list-table::
   :widths: 35 65

   * - **Pair format**
     - Concatenation without separator: ``'BTC'`` + ``'USDT'`` → ``'BTCUSDT'``.
   * - **Candles per request**
     - 1 000 (endpoint: ``GET /v5/market/kline``).
   * - **Minimum span**
     - 60 seconds (1 minute).
   * - **Trade history**
     - Recent trades **only** — up to the last 1 000 trades.  The Bybit public
       API does not support deep historical pagination.
   * - **Order book depth**
     - Snapshot at 1–200 levels per side (spot category).
   * - **Authentication**
     - Not required for public data.

Quick example
-------------

.. code-block:: python

   from dccd.histo_dl import FromBybit

   obj = FromBybit('/data/crypto/', 'BTC', span=3600, fiat='USDT')

   # Full OHLCV history (pagination supported)
   obj.import_data(start='2024-01-01 00:00:00', end='2024-12-31 00:00:00')
   obj.save(form='parquet')

   # Only recent trades are available (≤ 1 000)
   obj.import_trades(start='last', end='now')
   obj.save_trades(form='parquet')
