#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2019-08-30 10:42:53
# @Last modified by: ArthurBernard
# @Last modified time: 2026-05-12

""" Module to download historical OHLCV, trades, and order book data.

Supports five exchanges via REST APIs: Binance, Bybit, Coinbase, Kraken,
and OKX.  All exchange classes inherit from
:class:`~dccd.histo_dl.exchange.ImportDataCryptoCurrencies` and expose
the same interface:

.. code-block:: python

    from dccd.histo_dl import FromBinance

    obj = FromBinance('/path/to/data/', 'BTC', 3600, fiat='USDT')

    # OHLCV — download and save as annual Parquet
    obj.import_data(start='2024-01-01 00:00:00', end='now').save(form='parquet')
    df = obj.get_data()                   # pandas DataFrame

    # Incremental update (resume from last saved timestamp)
    obj.import_data(start='last', end='now').save(form='parquet')

    # Trades (Binance/Kraken support full history; Bybit/Coinbase recent only)
    obj.import_trades(start='2024-01-01', end='2024-01-02').save_trades()
    df_trades = obj.trades_df             # columns: tid, timestamp, price, amount, type

    # Order book snapshot
    obj.import_orderbook(depth=50).save_orderbook()
    df_book = obj.orderbook_df            # columns: side, price, amount, count

Data are stored via :class:`~dccd.storage.DataStore` under::

    {data_path}/{exchange}/ohlc/{pair}/{span}/YYYY.parquet
    {data_path}/{exchange}/trades/{pair}/YYYY-MM-DD.parquet
    {data_path}/{exchange}/orderbook/{pair}/YYYY-MM-DD.parquet

.. currentmodule:: dccd.histo_dl

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
