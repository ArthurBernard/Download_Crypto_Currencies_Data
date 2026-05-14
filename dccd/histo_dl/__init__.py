#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2019-08-30 10:42:53
# @Last modified by: ArthurBernard
# @Last modified time: 2026-05-12

""" Module to download historical data.

Module to download historical data (ohlc, trades, etc.) and automatically
update the database. *Currently supports Binance, Coinbase, and Kraken.*

The 'histo_dl' module contains a base class and three exchange classes to
download and update data.

The three classes are ``FromBinance``, ``FromCoinbase``, and ``FromKraken``.
All have the same methods and almost the same parameters:

- __init__(path, crypto, span, fiat(optional), form(optional)):
    Initialisation with path is the path where save the data (string), crypto
    is a crypto currency (string) and span is the interval time between each
    observation in seconds (integer) or can be a string as 'hourly', 'daily',
    etc. The optional parameters are fiat the second currency (default is
    'USD') and form the format to save the data (default is 'xlsx').

- import_data(start, end):
    Download data with start and end the timestamp (integer) or the date and
    time (string as 'yyyy-mm-dd hh:mm:ss'), respectively of the first
    observation and the last observation (default are special parameters
    start='last' and end='now'). Note: Kraken does not support the end
    parameter and returns only the last thousand observations.

- save(form(optional), by(optional)):
    Save the data with form the format of the saved data (default is 'xlsx')
    and by is the "size" of each saved file (default is 'Y' as an entire year).

- get_data():
    returns the data frame without any parameter.

Method chaining is available for these classes.

.. currentmodule:: dccd.histo_dl

.. toctree::
   :maxdepth: 1
   :caption: Contents

   histo_dl.binance
   histo_dl.coinbase
   histo_dl.kraken

"""

# Built-in packages

# Third party packages

# Local packages
from . import binance, bybit, coinbase, exchange, kraken, okx
from .binance import *
from .bybit import *
from .coinbase import *
from .kraken import *
from .okx import *

__all__ = ['exchange']
__all__ += binance.__all__
__all__ += bybit.__all__
__all__ += coinbase.__all__
__all__ += kraken.__all__
__all__ += okx.__all__
