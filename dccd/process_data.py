#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2019-08-06 15:25:49
# @Last modified by: ArthurBernard
# @Last modified time: 2019-08-07 17:05:56

# Built-in packages
import time

# External packages
import numpy as np
import pandas as pd

# Local packages

__all__ = ['set_marketdepth', 'set_trades']


def set_marketdepth(book, t=None):
    """ Set a market depth dataframe with list of order books.

    Parameters
    ----------
    orderbook : dict
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
    df = pd.DataFrame([{'price': k, 'amount': a} for k, a in book.items()],
                      dtype=np.float64)
    df = df.sort_index().reset_index(drop=True)
    bid_idx = df.amount > 0.
    ask_idx = df.amount < 0.
    bid = df.loc[bid_idx]
    ask = df.loc[ask_idx]

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
    """ Set a ohlc dataframe with list of order books.

    Parameters
    ----------
    orderbook : dict
        Hiorical trades tick by tick as dict.

    Returns
    -------
    pd.DataFrame
        Historical trades tick by tick as dataframe.

    """
    # Set dataframe
    df = pd.DataFrame(trades)

    return df.sort_values('tid').reset_index(drop=True)
