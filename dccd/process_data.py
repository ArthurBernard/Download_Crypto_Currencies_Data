#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2019-08-06 15:25:49
# @Last modified by: ArthurBernard
# @Last modified time: 2026-05-24

""" Functions to clean, sort and other process data. """

# Built-in packages
import time

# External packages
import numpy as np
import polars as pl

__all__ = ['set_marketdepth', 'set_ohlc', 'set_orders', 'set_trades']


def set_orders(orders, t=None):
    """ Set a dataframe with list of each order.

    Parameters
    ----------
    orders : list
        Each orders in a list.

    Returns
    -------
    pl.DataFrame
        List of orders as dataframe.

    """
    if t is None:
        t = int(time.time())

    df = pl.DataFrame(orders, schema_overrides={k: pl.Float64 for k in orders[0]})
    return df.with_columns(pl.lit(t).alias('timestamp'))


def set_marketdepth(book, t=None):
    """ Set a market depth dataframe with list of order books.

    Parameters
    ----------
    book : dict
        Orderbook as dict, where keys is the price (positive=bid, negative=ask)
        and value is the amount.

    Returns
    -------
    pl.DataFrame
        Order book as flat dataframe with columns
        ``['TS', 'side', 'rank', 'price', 'cum_amount', 'vwab']``.

    """
    if t is None:
        t = int(time.time())

    bid_keys = sorted([float(k) for k, v in book.items() if float(v) > 0], reverse=True)
    ask_keys = sorted([float(k) for k, v in book.items() if float(v) < 0])

    def _side_rows(keys, side):
        amounts = np.array([float(book[str(int(k)) if k == int(k) else str(k)]) for k in keys])
        if side == 'ask':
            amounts = np.abs(amounts)
        cum = np.cumsum(amounts)
        vwab = np.cumsum(amounts * np.abs(keys)) / cum
        return [
            {'TS': t, 'side': side, 'rank': i, 'price': float(k),
             'cum_amount': float(c), 'vwab': float(v)}
            for i, (k, c, v) in enumerate(zip(keys, cum, vwab))
        ]

    rows = _side_rows(bid_keys, 'bid') + _side_rows(ask_keys, 'ask')
    if not rows:
        return pl.DataFrame(schema={'TS': pl.Int64, 'side': pl.Utf8, 'rank': pl.Int64,
                                    'price': pl.Float64, 'cum_amount': pl.Float64,
                                    'vwab': pl.Float64})
    return pl.DataFrame(rows)


def set_trades(trades):
    """ Set a dataframe with list of trades.

    Parameters
    ----------
    trades : list
        Historical trades tick by tick as list.

    Returns
    -------
    pl.DataFrame
        Historical trades tick by tick as dataframe.

    """
    return pl.DataFrame(trades).sort('tid')


def set_ohlc(trades, ts=60):
    """ Aggregate and set a dataframe with list of trades.

    Parameters
    ----------
    trades : list
        Historical trades tick by tick as list.
    ts : int, optional
        Timestep in seconds to aggregate data, default is 60.

    Returns
    -------
    pl.DataFrame
        Aggregated trades as OHLC dataframe with columns
        ``['TS', 'open', 'high', 'low', 'close', 'volume']``.

    """
    df = pl.DataFrame(trades).sort('tid')
    df = df.with_columns((pl.col('timestamp') / 1000).alias('ts_sec'))
    df = df.with_columns(
        pl.from_epoch(pl.col('ts_sec').cast(pl.Int64), time_unit='s').alias('ts_dt')
    )
    result = (
        df.sort('ts_dt')
        .group_by_dynamic('ts_dt', every=f'{ts}s', closed='left', start_by='datapoint')
        .agg(
            pl.col('price').first().alias('open'),
            pl.col('price').max().alias('high'),
            pl.col('price').min().alias('low'),
            pl.col('price').last().alias('close'),
            pl.col('amount').sum().alias('volume'),
        )
        .with_columns(
            pl.col('ts_dt').dt.epoch(time_unit='s').alias('TS')
        )
        .drop('ts_dt')
        .select(['TS', 'open', 'high', 'low', 'close', 'volume'])
    )
    return result
