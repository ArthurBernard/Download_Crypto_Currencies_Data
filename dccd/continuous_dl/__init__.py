#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2019-08-30 09:46:41
# @Last modified by: ArthurBernard
# @Last modified time: 2019-09-03 22:26:18

""" Module to download continuously data.

Module to download continuously data (orderbook, trades, etc.) and update
automatically the database. Supports Binance, Bitfinex, Bitmex, Bybit,
Kraken, and OKX exchanges via WebSocket.

.. currentmodule:: dccd.continuous_dl

.. toctree::
   :maxdepth: 1
   :caption: Contents:

   continuous_dl.binance
   continuous_dl.bitfinex
   continuous_dl.bitmex
   continuous_dl.bybit
   continuous_dl.kraken
   continuous_dl.okx

"""

# Built-in packages

# Third party packages

# Local packages
from . import binance, bitfinex, bitmex, bybit, exchange, kraken, okx
from .binance import *
from .bitfinex import *
from .bitmex import *
from .bybit import *
from .kraken import *
from .okx import *

__all__ = ['exchange']
__all__ += binance.__all__
__all__ += bitfinex.__all__
__all__ += bitmex.__all__
__all__ += bybit.__all__
__all__ += kraken.__all__
__all__ += okx.__all__
