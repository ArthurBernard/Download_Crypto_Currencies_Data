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
