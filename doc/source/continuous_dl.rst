---------------------------------------------------
 Continuous Downloader (:mod:`dccd.continuous_dl`)
---------------------------------------------------

.. automodule:: dccd.continuous_dl
   :no-members:
   :no-inherited-members:
   :no-special-members:

High level API
--------------

.. autosummary::
   :toctree: generated/

   binance.get_data_binance -- download data from Binance exchange and update the database
   binance.get_orderbook_binance -- download order book from Binance exchange and update the database
   binance.get_trades_binance -- download trades from Binance exchange and update the database
   bitfinex.get_data_bitfinex -- download data from Bitfinex exchange and update the database
   bitfinex.get_orderbook_bitfinex -- download order book from Bitfinex exchange and update the database
   bitfinex.get_trades_bitfinex -- download trades from Bitfinex exchange and update the database
   bitmex.get_data_bitmex -- download data from Bitmex exchange and update the database
   bitmex.get_orderbook_bitmex -- download order book from Bitmex exchange and update the database
   bitmex.get_trades_bitmex -- download trades from Bitmex exchange and update the database
   bybit.get_data_bybit -- download data from Bybit exchange and update the database
   bybit.get_orderbook_bybit -- download order book from Bybit exchange and update the database
   bybit.get_trades_bybit -- download trades from Bybit exchange and update the database
   kraken.get_data_kraken -- download data from Kraken exchange and update the database
   kraken.get_orderbook_kraken -- download order book from Kraken exchange and update the database
   kraken.get_trades_kraken -- download trades from Kraken exchange and update the database
   okx.get_data_okx -- download data from OKX exchange and update the database
   okx.get_orderbook_okx -- download order book from OKX exchange and update the database
   okx.get_trades_okx -- download trades from OKX exchange and update the database

Low level API
-------------

.. autosummary::
   :toctree: generated/

   binance.DownloadBinanceData -- basis object to download data from Binance client websocket API
   bitfinex.DownloadBitfinexData -- basis object to download data from Bitfinex client websocket API
   bitmex.DownloadBitmexData -- basis object to download data from Bitmex client websocket API
   bybit.DownloadBybitData -- basis object to download data from Bybit client websocket API
   kraken.DownloadKrakenData -- basis object to download data from Kraken client websocket API
   okx.DownloadOKXData -- basis object to download data from OKX client websocket API
