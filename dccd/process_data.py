#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2019-08-06 15:25:49
# @Last modified by: ArthurBernard
# @Last modified time: 2019-08-30 09:42:40

""" Functions to clean, sort and other process data. """

# Built-in packages
import time

# External packages
import numpy as np
import pandas as pd

# Local packages

__all__ = ['set_marketdepth', 'set_ohlc', 'set_orders', 'set_trades']


def set_orders(orders, t=None):
    """ Set a dataframe with list of each order.

    Parameters
    ----------
    orders : list
        Each orders in a list.

    Returns
    -------
    pd.DataFrame
        List of orders as dataframe.

    """
    if t is None:
        t = int(time.time())

    # Set dataframe
    df = pd.DataFrame(orders, dtype=np.float64)
    df.loc[:, 'timestamp'] = t

    return df


def set_marketdepth(book, t=None):
    """ Set a market depth dataframe with list of order books.

    Parameters
    ----------
    book : dict
        Orderbook as dict, where keys is the price and value is the amount.

    Returns
    -------
    pd.DataFrame
        Order book as dataframe.

    """
    if t is None:
        t = int(time.time())

    # Set dataframe
    # df = pd.DataFrame(book, dtype=np.float64)
    # keys = sorted(book.keys(), reverse=True)
    keys = sorted([i for i in book.keys() if book[i] > 0], reverse=True)
    keys += sorted([i for i in book.keys() if book[i] < 0])
    df = pd.DataFrame([{'price': k, 'amount': book[k]} for k in keys],
                      dtype=np.float64)
    df = df.sort_index().reset_index(drop=True)
    # df.loc[df.amount > 0] = df.loc[df.amount > 0].iloc[::-1]
    bid_idx = df.amount > 0.
    ask_idx = df.amount < 0.
    bid = df.loc[bid_idx]
    ask = df.loc[ask_idx]  # .iloc[::-1]

    # Set bid cumulative amount
    df.loc[bid_idx, 'cum_amount'] = np.cumsum(bid.amount.values)
    df.loc[bid_idx, 'vwab'] = np.cumsum(bid.amount.values * bid.price.values)
    df.loc[bid_idx, 'vwab'] /= df.loc[bid_idx, 'cum_amount'].values

    # Set ask cumulative amount
    df.loc[ask_idx, 'cum_amount'] = np.cumsum(ask.amount.values)
    df.loc[ask_idx, 'vwab'] = np.cumsum(ask.amount.values * ask.price.values)
    df.loc[ask_idx, 'vwab'] /= df.loc[ask_idx, 'cum_amount'].values

    df = df.drop(columns=['amount'])

    asks, bids = df.loc[ask_idx], df.loc[bid_idx]

    # Set index
    n = len(bids.columns)
    bid_depth, ask_depth = bids.index.size, asks.index.size
    bid_slice = pd.IndexSlice[t, 'bid', :]
    ask_slice = pd.IndexSlice[t, 'ask', :]

    # Create dataframe
    df = pd.DataFrame(index=[[t] * 2 * n, ['bid'] * n + ['ask'] * n,
                             list(bids.columns) + list(asks.columns)],
                      columns=range(max(bid_depth, ask_depth)))

    # Set data
    df.loc[bid_slice, 0: bid_depth - 1] = bids.values.T
    df.loc[ask_slice, 0: ask_depth - 1] = np.abs(asks.values.T)

    return df


def set_trades(trades):
    """ Set a dataframe with list of trades.

    Parameters
    ----------
    trades : list
        Historical trades tick by tick as list.

    Returns
    -------
    pd.DataFrame
        Historical trades tick by tick as dataframe.

    """
    # Set dataframe
    df = pd.DataFrame(trades)

    return df.sort_values('tid').reset_index(drop=True)


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
    pd.DataFrame
        Aggregated trades as OHLC, dataframe is indexed by timestamp and
        columns contains 'open', 'high', 'low', 'close', and 'volume'.

    """
    df = pd.DataFrame(trades).sort_values('tid').reset_index(drop=True)
    df.timestamp = df.timestamp / 1000
    t0, t1 = int(df.timestamp.iloc[0] // ts * ts), int(df.timestamp.iloc[-1])
    db = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'],
                      index=range(t0, t1 + ts, ts))

    for t in np.arange(t0, t1 + ts, ts):
        data = df.set_index('timestamp').loc[t: t + ts, :]

        if data.empty:

            continue

        db.loc[t, 'open'] = data.price.iloc[0]
        db.loc[t, 'high'] = data.price.max()
        db.loc[t, 'low'] = data.price.min()
        db.loc[t, 'close'] = data.price.iloc[-1]
        db.loc[t, 'volume'] = data.amount.sum()

    return db
