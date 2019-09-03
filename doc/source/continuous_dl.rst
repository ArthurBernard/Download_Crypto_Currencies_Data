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

   bitfinex.get_data_bitfinex -- download data from Bitfinex exchange and update the database
   bitfinex.get_orderbook_bitfinex -- download order book from Bitfinex exchange and update the database
   bitfinex.get_trades_bitfinex -- download trades from Bitfinex exchange and update the database
   bitmex.get_data_bitmex -- download data from Bitmex exchange and update the database
   bitmex.get_orderbook_bitmex -- download order book from Bitmex exchange and update the database
   bitmex.get_trades_bitmex -- download trades from Bitmex exchange and update the database

Low level API
-------------

.. autosummary::
   :toctree: generated/

   bitfinex.DownloadBitfinexData -- basis object to download data from Bitfinex client websocket API
   bitmex.DownloadBitmexData -- basis object to download data from Bitmex client websocket API
