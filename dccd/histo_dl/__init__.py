#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2019-08-30 10:42:53
# @Last modified by: ArthurBernard
# @Last modified time: 2019-09-03 22:45:46

""" Module to download historical data.

Module to download historical data (ohlc, trades, etc.) and automatically
update the database. *Currently only supports Binance, GDax, Kraken and
Poloniex exchanges.*

Only Poloniex allow you to download old historical data.

The 'histo_dl' module contains a main class and four classes to download
and update data for each exchange.

The four classes to download data are ``FromBinance``, ``FromGDax``,
``FromKraken`` and ``FromPoloniex``. All have the same methods and almost
the same parameters:

- __init__(path, crypto, span, fiat(optional), form(optional)):
    Initialisation with path is the path where save the data (string), crypto
    is a crypto currency (string) and span is the interval time between each
    observation in seconds (integer) or can be a string as 'hourly', 'daily',
    etc. (see details on the doc string). The optional parameters are fiat the
    second currency (default is 'USD' and 'USDT' for poloniex and binance) and
    form the format to save the data (default is 'xlsx').

- import_data(start, end):
    Download data with start and end the timestamp (integer) or the date and
    time (string as 'yyyy-mm-dd hh:mm:ss'), respectively of the first
    observation and the last observation (default are special parameters
    start='last' allow the last data saved and end='now' allow the last
    observation available). Exclusion: Kraken don't allow the end parameter and
    provide only the thousand last observations.

- save(form(optional), by(optional)):
    Save the data with form the format of the saved data (default is 'xlsx')
    and by is the "size" of each saved file (default is 'Y' as an entire year).
    Exclusion: This optional parameters are in progress, let the default
    parameter for the moment, other are not allow.

- get_data():
    returns the data frame without any parameter.

Method chaining is available for these classes.

.. currentmodule:: dccd.histo_dl

.. toctree::
   :maxdepth: 1
   :caption: Contents

   histo_dl.binance
   histo_dl.gdax
   histo_dl.kraken
   histo_dl.poloniex

"""

# Built-in packages

# Third party packages

# Local packages
from . import exchange
from . import binance
from .binance import *
from . import gdax
from .gdax import *
from . import kraken
from .kraken import *
from . import poloniex
from .poloniex import *

__all__ = ['exchange']
__all__ += binance.__all__
__all__ += gdax.__all__
__all__ += kraken.__all__
__all__ += poloniex.__all__
