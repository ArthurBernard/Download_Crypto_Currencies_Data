"""Canonical data records — OHLCBar, Trade, OrderBookSnapshot."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

__all__ = [
    "OHLCBar",
    "Trade",
    "OrderBookLevel",
    "OrderBookSnapshot",
    "FundingRate",
    "OpenInterest",
]


class OHLCBar(BaseModel, frozen=True):
    """One OHLCV candle bar.

    Attributes
    ----------
    ts : int
        Open-time of the bar, **nanoseconds UTC**, aligned to *span*.
    open, high, low, close : float
        Price values.
    volume : float
        Base asset volume.
    quote_volume : float or None
        Quote asset volume (not always available).
    trades : int or None
        Number of individual trades in the bar (not always available).

    Examples
    --------
    >>> bar = OHLCBar(ts=1_000_000_000_000_000_000, open=1.0, high=2.0, low=0.5, close=1.5, volume=10.0)
    >>> bar.ts
    1000000000000000000
    """

    ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float | None = None
    trades: int | None = None


class Trade(BaseModel, frozen=True):
    """Single trade (tick).

    Attributes
    ----------
    ts : int
        Trade timestamp, **nanoseconds UTC**.
    price : float
        Execution price.
    amount : float
        Trade size (base asset).
    side : Literal['buy', 'sell'] or None
        Taker side. ``None`` when not provided by the exchange.
    tid : str or None
        Trade ID as string (some exchanges use non-integer IDs).

    Examples
    --------
    >>> t = Trade(ts=1_000_000_000_000_000_000, price=50000.0, amount=0.1, side='buy')
    >>> t.price
    50000.0
    """

    ts: int
    price: float
    amount: float
    side: Literal["buy", "sell"] | None = None
    tid: str | None = None


class OrderBookLevel(BaseModel, frozen=True):
    """One price level in an order book.

    Examples
    --------
    >>> lvl = OrderBookLevel(price=50000.0, amount=1.5)
    >>> lvl.price
    50000.0
    """

    price: float
    amount: float
    count: int | None = None


class OrderBookSnapshot(BaseModel, frozen=True):
    """A complete (or delta) order book state at a point in time.

    Attributes
    ----------
    ts : int
        Snapshot timestamp, **nanoseconds UTC**.
    bids : list[OrderBookLevel]
        Bid levels, sorted descending by price.
    asks : list[OrderBookLevel]
        Ask levels, sorted ascending by price.
    is_snapshot : bool
        ``True`` = full state; ``False`` = incremental delta to apply locally.

    Examples
    --------
    >>> snap = OrderBookSnapshot(ts=1_000_000_000_000_000_000, bids=[], asks=[])
    >>> snap.is_snapshot
    True
    """

    ts: int
    bids: list[OrderBookLevel]
    asks: list[OrderBookLevel]
    is_snapshot: bool = True


class FundingRate(BaseModel, frozen=True):
    """One realized perpetual-futures funding event.

    Attributes
    ----------
    ts : int
        Funding time, **nanoseconds UTC**.
    rate : float
        Realized funding rate for this event (e.g. ``0.0001`` = 0.01%).
    mark_price : float or None
        Mark price at funding time (not always available).

    Examples
    --------
    >>> f = FundingRate(ts=1_000_000_000_000_000_000, rate=0.0001)
    >>> f.mark_price is None
    True
    """

    ts: int
    rate: float
    mark_price: float | None = None


class OpenInterest(BaseModel, frozen=True):
    """One open-interest observation for a derivative instrument.

    Span-typed like OHLC: one row per fixed-cadence bucket (e.g. hourly),
    rather than one row per event like :class:`FundingRate`.

    Attributes
    ----------
    ts : int
        Observation time, **nanoseconds UTC**, aligned to the requested span.
    open_interest : float
        Open interest in contracts/base units.
    open_interest_value : float or None
        Open interest expressed as notional value (quote units), when the
        exchange provides it. ``None`` when not available.

    Examples
    --------
    >>> oi = OpenInterest(ts=1_000_000_000_000_000_000, open_interest=5000.0)
    >>> oi.open_interest_value is None
    True
    """

    ts: int
    open_interest: float
    open_interest_value: float | None = None
