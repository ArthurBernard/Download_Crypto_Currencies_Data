#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2019-08-30 09:46:41
# @Last modified by: ArthurBernard
# @Last modified time: 2019-09-03 22:26:18

""" Module to download continuously data.

Module to download continuously data (orderbook, trades, etc.) and update
automatically the database. *Currently only supports Bitfinex and Bitmex
exchanges.*

.. currentmodule:: dccd.continuous_dl

.. toctree::
   :maxdepth: 1
   :caption: Contents:

   continuous_dl.bitfinex
   continuous_dl.bitmex

"""

# Built-in packages

# Third party packages

# Local packages
from . import exchange
from . import bitfinex
from .bitfinex import *
from . import bitmex
from .bitmex import *

__all__ = ['exchange']
__all__ += bitfinex.__all__
__all__ += bitmex.__all__
