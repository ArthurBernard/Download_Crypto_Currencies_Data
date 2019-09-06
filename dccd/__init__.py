#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2019-07-26 16:54:02
# @Last modified by: ArthurBernard
# @Last modified time: 2019-09-06 09:01:33

""" This is `dccd` package.

It allows you to download data (prices, volumes, trades, orderbooks, etc.)
from crypto-currency exchanges (currently only Binance, Bitfinex, Bitmex, GDAX,
Kraken and Poloniex).

"""

__version__ = "1.1.1"

# ======= #
#  Tools  #
# ======= #

# from . import time_tools
# from .time_tools import *
# from . import io_tools
# from .io_tools import *
# from . import process_data
# from .process_data import *
# from . import tools
from .tools import date_time, io
# from .tools import *

# ===== #
#  New  #
# ===== #

from . import continuous_dl
from .continuous_dl import *
from . import histo_dl
from .histo_dl import *

# from . import bitfinex
# from .bitfinex import *
# from . import bitmex
# from .bitmex import *

# ===== #
#  Old  #
# ===== #

# from . import exchange
# from .exchange import ImportDataCryptoCurrencies
# from . import kraken
# from .kraken import FromKraken
# from . import gdax
# from .gdax import FromGDax
# from . import binance
# from .binance import FromBinance
# from . import poloniex
# from .poloniex import FromPoloniex

# __all__ = time_tools.__all__
__all__ = ['date_time']
__all__ += ['io']
__all__ += ['process_data']
# __all__ += tools.__all__

# __all__ += bitfinex.__all__
# __all__ += bitmex.__all__
__all__ += continuous_dl.__all__
__all__ += histo_dl.__all__

# __all__ += exchange.__all__
# __all__ += kraken.__all__
# __all__ += binance.__all__
# __all__ += gdax.__all__
# __all__ += poloniex.__all__
