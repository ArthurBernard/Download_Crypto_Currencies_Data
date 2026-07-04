"""DataType — closed enum for the three core data types."""

from __future__ import annotations

from enum import Enum

__all__ = ["DataType"]


class DataType(str, Enum):
    """Core data types supported by dccd v3.

    The enum is intentionally closed. Extensions (open interest, liquidations)
    will require adding a new member here AND implementing the corresponding
    domain models and source protocols — never bypassed by string comparison.
    ``FUNDING`` (realized perpetual-futures funding rate) is the first
    derivative-market data type added on top of the original three.

    Examples
    --------
    >>> DataType.OHLC
    <DataType.OHLC: 'ohlc'>
    >>> DataType('trades')
    <DataType.TRADES: 'trades'>
    >>> DataType('funding')
    <DataType.FUNDING: 'funding'>
    """

    OHLC = "ohlc"
    TRADES = "trades"
    ORDERBOOK = "orderbook"
    FUNDING = "funding"
