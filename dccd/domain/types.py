"""DataType — closed enum for the three core data types."""

from __future__ import annotations

from enum import Enum

__all__ = ["DataType"]


class DataType(str, Enum):
    """Core data types supported by dccd v3.

    The enum is intentionally closed. Extensions (liquidations, …) will
    require adding a new member here AND implementing the corresponding
    domain models and source protocols — never bypassed by string comparison.
    ``FUNDING`` (realized perpetual-futures funding rate) and
    ``OPEN_INTEREST`` (span-typed, like OHLC) are the derivative-market data
    types added on top of the original three.

    Examples
    --------
    >>> DataType.OHLC
    <DataType.OHLC: 'ohlc'>
    >>> DataType('trades')
    <DataType.TRADES: 'trades'>
    >>> DataType('funding')
    <DataType.FUNDING: 'funding'>
    >>> DataType('open_interest')
    <DataType.OPEN_INTEREST: 'open_interest'>
    """

    OHLC = "ohlc"
    TRADES = "trades"
    ORDERBOOK = "orderbook"
    FUNDING = "funding"
    OPEN_INTEREST = "open_interest"
