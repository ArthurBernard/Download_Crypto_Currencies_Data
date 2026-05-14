#!/usr/bin/env python3
# coding: utf-8

""" Pydantic models for validating exchange API responses. """

from pydantic import BaseModel

__all__ = ['OHLCBar', 'Trade', 'OrderBookEntry']


class OHLCBar(BaseModel):
    """ OHLCV bar returned by exchange REST APIs.

    Parameters
    ----------
    date : float
        Unix timestamp (seconds).
    open, high, low, close : float
        Price values.
    volume : float
        Base asset volume.
    quoteVolume : float
        Quote asset volume.
    weightedAverage : float, optional
        VWAP (Kraken only).

    """

    date: float
    open: float
    high: float
    low: float
    close: float
    volume: float
    quoteVolume: float
    weightedAverage: float | None = None


class Trade(BaseModel):
    """ Individual trade returned by WebSocket streams.

    Parameters
    ----------
    tid : int
        Trade ID.
    timestamp : float
        Unix timestamp (seconds).
    price : float
        Trade price.
    amount : float
        Trade size (base asset).
    type : str, optional
        'buy' or 'sell'.

    """

    tid: int
    timestamp: float
    price: float
    amount: float
    type: str | None = None


class OrderBookEntry(BaseModel):
    """ Single order book entry from WebSocket streams.

    Parameters
    ----------
    price : str
        Price level as string key.
    count : int
        Number of orders at this level.
    amount : float
        Total size at this level.

    """

    price: str
    count: int
    amount: float
