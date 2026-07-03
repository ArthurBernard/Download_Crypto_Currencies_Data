"""Capability — describes what a source can do."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel

from dccd.domain.types import DataType

__all__ = ["Capability"]


class Capability(BaseModel, frozen=True):
    """Declares one atomic capability of a source adapter.

    Attributes
    ----------
    data_type : DataType
    transport : 'rest' or 'ws'
    mode : 'historical' or 'live'
    history : 'full' or 'recent'
        For historical REST: ``'full'`` means the source covers the entire
        available exchange history; ``'recent'`` means only the last N bars
        are accessible (e.g. Kraken OHLC = 720 recent bars).
    max_per_request : int or None
        Maximum items per API call (drives Paginator step size).
    page_direction : 'forward', 'backward', or None
        Pagination direction. ``None`` for single-shot / WS sources.
    spans : list[int] or None
        Supported OHLC spans in seconds. ``None`` = span list not constrained.
    max_depth : int or None
        Maximum order book depth.
    depths : list[int] or None
        Discrete order-book depths the channel accepts (e.g. Kraken WS v2:
        ``[10, 25, 100, 500, 1000]``). ``None`` = not constrained. The stream
        operation snaps a requested depth to the nearest declared value — an
        undeclared depth would be silently rejected by the exchange and leave
        a "live" stream that never writes anything.
    auth_required : bool
        Whether this capability requires authentication.
    markets : list[str] or None
        Non-spot markets this capability serves (e.g. ``['perp']``,
        ``['perp', 'quarter']``). ``None`` means spot-only — the honest
        default for every existing declaration; a source must opt in
        explicitly before a derivative-market target is accepted.
    recent_window_s : int or None
        Length, in seconds, of the recent window served when ``history``
        is ``'recent'`` and that window is time-bound rather than
        bar-count-bound (``max_per_request`` bars). ``None`` when the
        recent window is bar-count-bound or not applicable. Used by the
        Binance open-interest adapter.

    Examples
    --------
    >>> from dccd.domain.types import DataType
    >>> cap = Capability(data_type=DataType.OHLC, transport='rest', mode='historical', max_per_request=1000)
    >>> cap.history
    'full'
    """

    data_type: DataType
    transport: Literal["rest", "ws"]
    mode: Literal["historical", "live"]
    history: Literal["full", "recent"] = "full"
    max_per_request: int | None = None
    page_direction: Literal["forward", "backward"] | None = None
    spans: list[int] | None = None
    max_depth: int | None = None
    depths: list[int] | None = None
    auth_required: bool = False
    markets: list[str] | None = None
    recent_window_s: int | None = None
