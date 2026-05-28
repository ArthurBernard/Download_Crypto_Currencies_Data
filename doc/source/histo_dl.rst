----------------------------------------------
 Historical Downloader (:mod:`dccd.histo_dl`)
----------------------------------------------

.. automodule:: dccd.histo_dl
   :no-members:
   :no-inherited-members:
   :no-special-members:

Downloads OHLCV candles, trades, and order book snapshots via exchange REST
APIs.  All exchange classes inherit from
:class:`~dccd.histo_dl.exchange.ImportDataCryptoCurrencies` and share a
fluent interface: ``import_data(start, end).save(form='parquet')``.

.. autosummary::
   :toctree: generated/

   binance.FromBinance -- historical downloader for Binance REST API
   coinbase.FromCoinbase -- historical downloader for Coinbase REST API
   kraken.FromKraken -- historical downloader for Kraken REST API
   bybit.FromBybit -- historical downloader for Bybit REST API
   okx.FromOKX -- historical downloader for OKX REST API
