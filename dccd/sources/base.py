"""Source protocols — fine-grained per (data_type × mode)."""

from __future__ import annotations

from collections.abc import AsyncIterator

from dccd.domain.capability import Capability
from dccd.domain.records import OHLCBar, OrderBookSnapshot, Trade
from dccd.domain.symbol import Symbol
from dccd.domain.types import DataType

__all__ = [
    "Source",
    "OHLCHistory",
    "TradesHistory",
    "OrderBookSnapshotREST",
    "OHLCLive",
    "TradesLive",
    "OrderBookLive",
]


class Source:
    """Base mixin for all source adapters.

    Adapters inherit from ``Source`` and one or more capability protocols.
    They declare capabilities and render exchange-specific symbol strings.
    """

    exchange: str = ""

    def capabilities(self) -> list[Capability]:
        """Return list of declared capabilities."""
        return []

    def render_symbol(self, s: Symbol) -> str:
        """Render a canonical Symbol to the exchange-specific string."""
        return str(s)

    def capability_for(
        self,
        data_type: DataType,
        transport: str,
        mode: str,
    ) -> Capability | None:
        for cap in self.capabilities():
            if (
                cap.data_type == data_type
                and cap.transport == transport
                and cap.mode == mode
            ):
                return cap
        return None


class OHLCHistory(Source):
    """Protocol: can fetch historical OHLC pages via REST."""

    async def fetch_ohlc_page(
        self,
        symbol: Symbol,
        span: int,
        start_ns: int,
        end_ns: int,
        limit: int,
    ) -> list[OHLCBar]:
        raise NotImplementedError


class TradesHistory(Source):
    """Protocol: can fetch historical trade pages via REST.

    Cursor contract: ``fetch_trades_page`` returns ``(trades, next_cursor)``.
    The *cursor* is an opaque, adapter-defined string used to continue inside
    the ``[start_ns, end_ns)`` window:

    - ``cursor=None`` on the first call — anchor on ``start_ns`` (or ``end_ns``
      for adapters that page backward).
    - ``next_cursor`` is ``None`` when the window is exhausted (the adapter
      returned a short/last page, or the next item would fall outside the
      window). Returning a non-``None`` cursor tells the paginator to call again.

    This lets the generic paginator drain a window completely — fixing the
    capped-single-page data loss that affected every liquid pair — without
    per-exchange chunking in the application layer.
    """

    async def fetch_trades_page(
        self,
        symbol: Symbol,
        start_ns: int,
        end_ns: int,
        limit: int,
        cursor: str | None = None,
    ) -> tuple[list[Trade], str | None]:
        raise NotImplementedError


class OrderBookSnapshotREST(Source):
    """Protocol: can fetch an order book snapshot via REST."""

    async def fetch_orderbook(
        self,
        symbol: Symbol,
        depth: int,
    ) -> OrderBookSnapshot:
        raise NotImplementedError


class OHLCLive(Source):
    """Protocol: can stream live OHLC bars via WebSocket."""

    def stream_ohlc(self, symbol: Symbol, span: int) -> AsyncIterator[OHLCBar]:
        raise NotImplementedError


class TradesLive(Source):
    """Protocol: can stream live trades via WebSocket."""

    def stream_trades(self, symbol: Symbol) -> AsyncIterator[Trade]:
        raise NotImplementedError


class OrderBookLive(Source):
    """Protocol: can stream live order book snapshots/deltas via WebSocket."""

    def stream_orderbook(self, symbol: Symbol, depth: int) -> AsyncIterator[OrderBookSnapshot]:
        raise NotImplementedError
