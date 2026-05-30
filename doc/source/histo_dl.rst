----------------------------------------------
 Historical Downloader (:mod:`dccd.histo_dl`)
----------------------------------------------

.. automodule:: dccd.histo_dl
   :no-members:
   :no-inherited-members:
   :no-special-members:

Downloads OHLCV candles, trade history, and order book snapshots from
exchange REST APIs.  All exchange classes share the same fluent interface
and inherit from :class:`~dccd.histo_dl.exchange.ImportDataCryptoCurrencies`.

Typical workflow
----------------

.. code-block:: python

   from dccd.histo_dl import FromBinance

   obj = FromBinance('/data/crypto/', 'BTC', span=3600, fiat='USDT')

   # Download and persist
   obj.import_data(start='2024-01-01 00:00:00', end='2024-12-31 00:00:00')
   obj.save(form='parquet')

   # Load back as a Polars DataFrame
   df = obj.get_data()

Resuming an interrupted download
---------------------------------

Pass ``start='last'`` to resume from the latest saved timestamp:

.. code-block:: python

   obj.import_data(start='last', end='now').save(form='parquet')

On the first run this downloads everything; on subsequent runs it resumes
without duplicate rows.  Combine with cron or the :doc:`daemon` for
automated incremental collection.

Supported spans
---------------

All exchanges support at least the spans below.  Exchange-specific pages
document additional intervals.

.. list-table::
   :header-rows: 1
   :widths: 15 10 14 14 14 14 14

   * - Span (s)
     - Label
     - Binance
     - Kraken
     - Bybit
     - OKX
     - Coinbase
   * - 60
     - 1m
     - ✓
     - ✓
     - ✓
     - ✓
     - ✓
   * - 300
     - 5m
     - ✓
     - ✓
     - ✓
     - ✓
     - ✓
   * - 900
     - 15m
     - ✓
     - ✓
     - ✓
     - ✓
     - ✓
   * - 3600
     - 1h
     - ✓
     - ✓
     - ✓
     - ✓
     - ✓
   * - 14400
     - 4h
     - ✓
     - ✓
     - ✓
     - ✓
     - ✓
   * - 86400
     - 1d
     - ✓
     - ✓
     - ✓
     - ✓
     - ✓

Each exchange has additional constraints (pair format, candles per request,
trade history availability) — see the exchange-specific pages below.

Exchange classes
----------------

.. autosummary::
   :toctree: generated/

   binance.FromBinance -- historical downloader for Binance REST API
   coinbase.FromCoinbase -- historical downloader for Coinbase REST API
   kraken.FromKraken -- historical downloader for Kraken REST API
   bybit.FromBybit -- historical downloader for Bybit REST API
   okx.FromOKX -- historical downloader for OKX REST API

.. toctree::
   :hidden:
   :caption: Exchanges

   histo_dl.binance
   histo_dl.coinbase
   histo_dl.kraken
   histo_dl.bybit
   histo_dl.okx
