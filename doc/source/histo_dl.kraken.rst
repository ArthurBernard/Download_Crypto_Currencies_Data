Kraken Historical Downloader (:mod:`dccd.histo_dl.kraken`)
==========================================================

.. automodule:: dccd.histo_dl.kraken
   :no-members:
   :no-inherited-members:
   :no-special-members:

:class:`~dccd.histo_dl.kraken.FromKraken` downloads OHLCV candles,
trade history, and order book snapshots from the Kraken public REST API.
No authentication is required.

Exchange specifics
------------------

.. list-table::
   :widths: 35 65

   * - **Pair format**
     - Kraken uses a prefix scheme: most pairs become ``'X' + crypto + 'Z' + fiat``
       (e.g. ``'BTC'`` + ``'USD'`` → ``'XXBTZUSD'``).  Stablecoins and a few
       majors skip the prefix (e.g. ``'BCHUSD'``).
       :meth:`~dccd.histo_dl.kraken.FromKraken.format_pair` resolves this
       automatically.
   * - **OHLCV endpoint limitation**
     - ``GET /0/public/OHLC`` does **not** accept an end timestamp — it always
       returns data up to *now*.  The ``end`` parameter of ``import_data()`` is
       ignored with a :class:`UserWarning`.
   * - **Candles per request**
     - 720 (Kraken returns up to 720 bars per call).
   * - **Trade history**
     - Full historical depth via ``GET /0/public/Trades``, paginated by Unix
       nanosecond timestamp.
   * - **Order book depth**
     - Snapshot at 1–500 price levels per side.
   * - **Daemon backfill strategy**
     - Because the OHLCV endpoint lacks pagination, the daemon uses
       :class:`~dccd.daemon.backfill.KrakenBackfill` which reconstructs OHLC
       bars from the full trade history and resamples them.
   * - **Authentication**
     - Not required for public data.

Quick example
-------------

.. code-block:: python

   from dccd.histo_dl import FromKraken

   # Pair format is handled automatically (BTC + USD → XXBTZUSD)
   obj = FromKraken('/data/crypto/', 'BTC', span=3600, fiat='USD')

   # 'end' is ignored for OHLCV — Kraken always fetches up to now
   obj.import_data(start='2024-01-01 00:00:00', end='2024-12-31 00:00:00')
   obj.save(form='parquet')

   # Full trade history with pagination
   obj.import_trades(start='2024-01-01 00:00:00', end='2024-06-30 00:00:00')
   obj.save_trades(form='parquet')
