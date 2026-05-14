====================================================
 Welcome to Download Crypto Currencies Data project
====================================================

This is the documentation of ``dccd`` package, a package to download crypto-currencies data from Binance, Coinbase, Kraken, Bybit, OKX, Bitfinex, and Bitmex.

Installation
------------

From pip:
   $ pip install dccd

From source:
   $ git clone https://github.com/ArthurBernard/Download_Crypto_Currencies_Data.git

   $ cd Download_Crypto_Currencies_Data

   $ pip install -e .

Presentation
------------

The ``dccd`` package allow you two main methods to download data. The first one is recommended to download data at high frequency (**minutely** or **tick by tick**), and the second one is recommended to download data at a lower frequency (**hourly** or **daily**):

- Continuous Downloader :mod:`dccd.continuous_dl`:
   Download and update continuously data (orderbook, trades tick by tick, ohlc, etc) and save it in a database. *Supports Bitfinex, Bitmex, and Bybit exchanges*.
- Historical Downloader :mod:`dccd.histo_dl`:
   Download historical data (ohlc, trades, etc.) and save it. *Supports Binance, Coinbase, Kraken, Bybit, and OKX exchanges*.

Contents
--------

.. toctree::
   :maxdepth: 2

   continuous_dl
   continuous_dl.bitfinex
   continuous_dl.bitmex
   continuous_dl.bybit
   histo_dl
   histo_dl.binance
   histo_dl.coinbase
   histo_dl.kraken
   histo_dl.bybit
   histo_dl.okx
   process_data
   tools
