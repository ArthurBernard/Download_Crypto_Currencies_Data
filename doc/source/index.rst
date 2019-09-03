====================================================
 Welcome to Download Crypto Currencies Data project 
====================================================

This is the documentation of ``dccd`` package, package to download crypto-currencies data from Binance, Bitfinex, Bitmex, GDax (Coinbase), Kraken and Poloniex.

Two main methods to download data:

- Continuous Downloader :mod:`dccd.continuous_dl`:
   Download and update continuously data (orderbook, trades tick by tick, ohlc, etc) and save it in a database. *Currently only support Bitfinex and Bitmex exchanges*.
- Historical Downloader :mod:`dccd.histo_dl`:
   Download historical data (ohlc, trades, etc.) and save it. *Currently only support Binance, GDax, Kraken and Poloniex exchanges*.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   continuous_dl
   histo_dl
   process_data
   tools
