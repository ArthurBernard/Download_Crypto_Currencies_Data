#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2019-07-26 16:54:02
# @Last modified by: ArthurBernard
# @Last modified time: 2026-05-12

""" This is `dccd` package.

It allows you to download data (prices, volumes, trades, orderbooks, etc.)
from crypto-currency exchanges (currently Binance, Bitfinex, Bitmex, Coinbase,
and Kraken).

"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("dccd")
except PackageNotFoundError:
    __version__ = "unknown"

__all__ = ['__version__']

# ======= #
#  Tools  #
# ======= #

# ===== #
#  New  #
# ===== #
from . import continuous_dl, histo_dl
from .continuous_dl import *
from .histo_dl import *
from .tools import date_time, io

__all__ += ['date_time']
__all__ += ['io']
__all__ += ['process_data']
__all__ += continuous_dl.__all__
__all__ += histo_dl.__all__
