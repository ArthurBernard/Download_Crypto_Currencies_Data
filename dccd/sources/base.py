"""Source protocols — fine-grained per (data_type × mode)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from dccd.domain.capability import Capability
from dccd.domain.records import OHLCBar, OrderBookSnapshot, Trade
from dccd.domain.symbol import Symbol
from dccd.domain.types import DataType

if TYPE_CHECKING:
    from dccd.transport.http import AsyncHTTPClient

__all__ = [
    "Source",
    "OHLCHistory",
    "TradesHistory",
    "OrderBookSnapshotREST",
    "OHLCLive",
    "TradesLive",
    "OrderBookLive",
    "default_http_client",
]


def default_http_client(exchange: str) -> "AsyncHTTPClient":
    """Build an :class:`~dccd.transport.http.AsyncHTTPClient` wired to the limiter.

    REST adapters call this to construct their default shared client so that
    every outbound request is throttled by the process-wide per-exchange
    :func:`~dccd.transport.ratelimit.shared_limiter`. Keeping the wiring here
    (rather than in each adapter) means a single seam controls proactive
    rate-limiting for all exchanges.

    Parameters
    ----------
    exchange : str
        Exchange name used to key the limiter bucket.

    Returns
    -------
    AsyncHTTPClient
    """
    from dccd.transport.http import AsyncHTTPClient
    from dccd.transport.ratelimit import shared_limiter

    return AsyncHTTPClient(exchange=exchange, limiter=shared_limiter())


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
        """Return the declared :class:`Capability` for this combination, or None."""
        for cap in self.capabilities():
            if (
                cap.data_type == data_type
                and cap.transport == transport
                and cap.mode == mode
            ):
                return cap
        return None

    @property
    def http_client(self) -> "AsyncHTTPClient | None":
        """Return the adapter's shared :class:`~dccd.transport.http.AsyncHTTPClient`.

        REST adapters store their client in ``self._http`` and return it here so
        callers (e.g. :func:`~dccd.application.operations.backfill`) can hold the
        context open for an entire multi-page operation — keeping ``_depth >= 1``
        across pages so no TCP/TLS re-handshake occurs between pages.

        WebSocket-only adapters return ``None`` (default).
        """
        http = getattr(self, "_http", None)
        if http is not None:
            from dccd.transport.http import AsyncHTTPClient
            if isinstance(http, AsyncHTTPClient):
                return http
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
        """Fetch up to *limit* OHLC bars of *span* seconds in ``[start_ns, end_ns]``."""
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
        """Fetch one page of trades; return ``(trades, next_cursor)`` (see class)."""
        raise NotImplementedError


class OrderBookSnapshotREST(Source):
    """Protocol: can fetch an order book snapshot via REST."""

    async def fetch_orderbook(
        self,
        symbol: Symbol,
        depth: int,
    ) -> OrderBookSnapshot:
        """Fetch a current order-book snapshot up to *depth* levels."""
        raise NotImplementedError


class OHLCLive(Source):
    """Protocol: can stream live OHLC bars via WebSocket."""

    def stream_ohlc(self, symbol: Symbol, span: int) -> AsyncIterator[OHLCBar]:
        """Yield live OHLC bars of *span* seconds over WebSocket."""
        raise NotImplementedError


class TradesLive(Source):
    """Protocol: can stream live trades via WebSocket."""

    def stream_trades(self, symbol: Symbol) -> AsyncIterator[Trade]:
        """Yield live trades over WebSocket."""
        raise NotImplementedError


class OrderBookLive(Source):
    """Protocol: can stream live order book snapshots/deltas via WebSocket."""

    def stream_orderbook(
        self,
        symbol: Symbol,
        depth: int,
        *,
        min_interval: float = 0.0,
    ) -> AsyncIterator[OrderBookSnapshot]:
        """Yield live order-book snapshots over WebSocket.

        Parameters
        ----------
        symbol : Symbol
        depth : int
            Maximum number of levels per side to include in each snapshot.
        min_interval : float, optional
            Minimum seconds between emitted snapshots.  ``0.0`` (default)
            preserves the legacy per-frame behaviour — every WS frame yields a
            snapshot.  Pass ``snapshot_interval`` from the job spec to move the
            throttle *upstream* so pydantic objects are only constructed for
            frames that will actually be saved.
        """
        raise NotImplementedError
