"""Pure data transformations — no I/O, used by adapters and derivation."""

from __future__ import annotations

from dccd.domain.records import OHLCBar, Trade
from dccd.domain.timeutils import align_ns

__all__ = ["aggregate_ohlc"]


def aggregate_ohlc(trades: list[Trade], span: int) -> list[OHLCBar]:
    """Aggregate trades into OHLC bars of *span* seconds.

    Parameters
    ----------
    trades : list[Trade]
        Input trades, sorted ascending by ``ts`` (nanoseconds UTC).
    span : int
        Bar duration in seconds.

    Returns
    -------
    list[OHLCBar]
        One bar per non-empty span window, ascending.

    Examples
    --------
    >>> from dccd.domain.records import Trade
    >>> trades = [
    ...     Trade(ts=1_000_000_000_000_000_000, price=100.0, amount=1.0, side='buy'),
    ...     Trade(ts=1_000_000_060_000_000_000, price=110.0, amount=2.0, side='sell'),
    ... ]
    >>> bars = aggregate_ohlc(trades, span=60)
    >>> len(bars)
    2
    """
    if not trades:
        return []

    buckets: dict[int, list[Trade]] = {}
    for trade in sorted(trades, key=lambda t: t.ts):
        bucket_ts = align_ns(trade.ts, span)
        buckets.setdefault(bucket_ts, []).append(trade)

    bars: list[OHLCBar] = []
    for ts_open, bucket in sorted(buckets.items()):
        prices = [t.price for t in bucket]
        volume = sum(t.amount for t in bucket)
        bars.append(
            OHLCBar(
                ts=ts_open,
                open=prices[0],
                high=max(prices),
                low=min(prices),
                close=prices[-1],
                volume=volume,
                trades=len(bucket),
            )
        )
    return bars
