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
    """ Individual trade from WebSocket streams or REST history endpoints.

    Parameters
    ----------
    tid : int or None
        Trade ID.  ``None`` when the exchange does not provide an integer ID
        (e.g. Kraken, Bybit).
    timestamp : float
        Unix timestamp (seconds).
    price : float
        Trade price.
    amount : float
        Trade size (base asset).
    type : str, optional
        ``'buy'`` or ``'sell'``.

    """

    tid: int | None = None
    timestamp: float
    price: float
    amount: float
    type: str | None = None


class OrderBookEntry(BaseModel):
    """ Single order book level from REST snapshot or WebSocket streams.

    Parameters
    ----------
    side : str
        ``'bid'`` or ``'ask'``.
    price : str
        Price level as a string (preserves precision).
    amount : float
        Total quantity available at this level.
    count : int or None
        Number of open orders at this level.  ``None`` when the exchange does
        not provide this information (e.g. Binance, Kraken).

    """

    side: str
    price: str
    amount: float
    count: int | None = None
