"""Binance source adapter — OHLC, trades, order book (REST + WS)."""

from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from typing import Any

from dccd.domain.capability import Capability
from dccd.domain.records import OHLCBar, OrderBookLevel, OrderBookSnapshot, Trade
from dccd.domain.symbol import Symbol
from dccd.domain.timeutils import binance_interval, s_to_ns
from dccd.domain.types import DataType
from dccd.sources.base import (
    OHLCHistory,
    OHLCLive,
    OrderBookLive,
    OrderBookSnapshotREST,
    TradesHistory,
    TradesLive,
    default_http_client,
)
from dccd.transport.http import AsyncHTTPClient
from dccd.transport.ws import WebSocketBase

__all__ = ["BinanceSource"]

logger = logging.getLogger(__name__)

_BASE_REST = "https://api.binance.com/api/v3"
_BASE_WS = "wss://stream.binance.com:9443/stream"


class BinanceSource(
    OHLCHistory,
    TradesHistory,
    OrderBookSnapshotREST,
    OHLCLive,
    TradesLive,
    OrderBookLive,
):
    """Binance source adapter (spot).

    The reference adapter — full historical depth and every live channel.

    - **Backfill**: OHLC (``klines``, 1 000/req), trades (``aggTrades``,
      cursor-paginated by ``fromId``), order-book snapshot (``depth``, ≤ 5 000).
    - **Stream**: OHLC (``kline``), trades (``aggTrade``), order book (``depth``).

    Adapters are not used directly — they are resolved by the engine from the
    registry. Drive them through :class:`dccd.Client` or the CLI.

    See Also
    --------
    dccd.Client : the public facade.
    dccd.sources.registry.SourceRegistry : adapter resolution.

    Examples
    --------
    >>> from dccd.sources.binance import BinanceSource
    >>> sorted({c.data_type.value for c in BinanceSource().capabilities()})
    ['ohlc', 'orderbook', 'trades']
    """

    exchange = "binance"

    def __init__(self, http: AsyncHTTPClient | None = None) -> None:
        self._http = http or default_http_client(self.exchange)
        self._owned_http = http is None

    def capabilities(self) -> list[Capability]:
        """Declared capabilities, one per (data type × transport × mode)."""
        return [
            Capability(
                data_type=DataType.OHLC, transport="rest", mode="historical",
                history="full", max_per_request=1000, page_direction="forward",
                spans=[60, 180, 300, 900, 1800, 3600, 7200, 14400, 21600, 28800,
                       43200, 86400, 259200, 604800, 2592000],
            ),
            Capability(
                data_type=DataType.TRADES, transport="rest", mode="historical",
                history="full", max_per_request=1000, page_direction="forward",
            ),
            Capability(
                data_type=DataType.ORDERBOOK, transport="rest", mode="historical",
                max_per_request=1, max_depth=5000,
            ),
            Capability(data_type=DataType.OHLC, transport="ws", mode="live"),
            Capability(data_type=DataType.TRADES, transport="ws", mode="live"),
            Capability(data_type=DataType.ORDERBOOK, transport="ws", mode="live", max_depth=20, depths=[5, 10, 20]),
        ]

    def render_symbol(self, s: Symbol) -> str:
        """Binance format: BTCUSDT (no separator)."""
        return f"{s.base}{s.quote}"

    async def fetch_ohlc_page(
        self,
        symbol: Symbol,
        span: int,
        start_ns: int,
        end_ns: int,
        limit: int,
    ) -> list[OHLCBar]:
        """Fetch one page of OHLC bars (see :meth:`~dccd.sources.base.OHLCHistory.fetch_ohlc_page`)."""
        interval = binance_interval(span)
        if not interval:
            return []
        params: dict[str, Any] = {
            "symbol": self.render_symbol(symbol),
            "interval": interval,
            "startTime": start_ns // 1_000_000,
            "endTime": end_ns // 1_000_000,
            "limit": limit,
        }
        async with self._http as client:
            data = await client.get(f"{_BASE_REST}/klines", params)

        return [
            OHLCBar(
                ts=int(e[0]) * 1_000_000,
                open=float(e[1]),
                high=float(e[2]),
                low=float(e[3]),
                close=float(e[4]),
                volume=float(e[5]),
                quote_volume=float(e[7]),
                trades=int(e[8]),
            )
            for e in data
        ]

    async def fetch_trades_page(
        self,
        symbol: Symbol,
        start_ns: int,
        end_ns: int,
        limit: int,
        cursor: str | None = None,
    ) -> tuple[list[Trade], str | None]:
        """Fetch one page of aggregate trades (cursor = ``fromId``).

        First call (``cursor=None``) is time-bounded on ``start_ns``; subsequent
        calls follow the ``fromId`` cursor. The next cursor is the last
        aggregate-trade id + 1, returned only while the page is full and the
        last trade is still inside the window.
        """
        sym = self.render_symbol(symbol)
        end_ms = end_ns // 1_000_000
        if cursor is None:
            params: dict[str, Any] = {
                "symbol": sym,
                "startTime": start_ns // 1_000_000,
                "endTime": end_ms,
                "limit": limit,
            }
        else:
            params = {"symbol": sym, "fromId": int(cursor), "limit": limit}

        async with self._http as client:
            data = await client.get(f"{_BASE_REST}/aggTrades", params)

        if not data:
            return [], None

        trades = [
            Trade(
                ts=int(e["T"]) * 1_000_000,
                price=float(e["p"]),
                amount=float(e["q"]),
                side="sell" if e["m"] else "buy",
                tid=str(e["a"]),
            )
            for e in data
        ]
        last_ts_ms = int(data[-1]["T"])
        next_cursor = (
            str(int(data[-1]["a"]) + 1)
            if len(data) >= limit and last_ts_ms < end_ms
            else None
        )
        return trades, next_cursor

    async def fetch_orderbook(self, symbol: Symbol, depth: int) -> OrderBookSnapshot:
        """Fetch a current order-book snapshot up to *depth* levels."""
        params = {"symbol": self.render_symbol(symbol), "limit": min(depth, 5000)}
        async with self._http as client:
            data = await client.get(f"{_BASE_REST}/depth", params)

        bids = [OrderBookLevel(price=float(b[0]), amount=float(b[1])) for b in data["bids"]]
        asks = [OrderBookLevel(price=float(a[0]), amount=float(a[1])) for a in data["asks"]]
        import time
        return OrderBookSnapshot(ts=s_to_ns(time.time()), bids=bids, asks=asks)

    def stream_ohlc(self, symbol: Symbol, span: int) -> AsyncIterator[OHLCBar]:
        """Stream live OHLC bars over WebSocket."""
        interval = binance_interval(span) or "1m"
        pair = self.render_symbol(symbol).lower()
        ws = _BinanceKlineWS(pair, interval)
        return ws.stream()

    def stream_trades(self, symbol: Symbol) -> AsyncIterator[Trade]:
        """Stream live trades over WebSocket."""
        pair = self.render_symbol(symbol).lower()
        ws = _BinanceTradeWS(pair)
        return ws.stream()

    def stream_orderbook(
        self,
        symbol: Symbol,
        depth: int,
        *,
        min_interval: float = 0.0,
    ) -> AsyncIterator[OrderBookSnapshot]:
        """Stream live order-book snapshots over WebSocket.

        Uses Binance's *partial book depth* stream, which pushes a fully sorted
        top-N snapshot — N is clamped to the supported {5, 10, 20}.
        """
        pair = self.render_symbol(symbol).lower()
        levels = 5 if depth <= 5 else 10 if depth <= 10 else 20
        ws = _BinanceDepthWS(pair, levels)
        return ws.stream(min_interval=min_interval)


class _BinanceKlineWS(WebSocketBase):
    def __init__(self, pair: str, interval: str) -> None:
        super().__init__(f"wss://stream.binance.com:9443/ws/{pair}@kline_{interval}")

    async def parse_message(self, raw: str | bytes) -> AsyncIterator[OHLCBar]:
        """Parse a raw WebSocket frame into domain records."""
        data = json.loads(raw)
        if data.get("e") != "kline":
            return
        k = data["k"]
        yield OHLCBar(
            ts=int(k["t"]) * 1_000_000,
            open=float(k["o"]),
            high=float(k["h"]),
            low=float(k["l"]),
            close=float(k["c"]),
            volume=float(k["v"]),
            quote_volume=float(k["q"]),
            trades=int(k["n"]),
        )


class _BinanceTradeWS(WebSocketBase):
    def __init__(self, pair: str) -> None:
        super().__init__(f"wss://stream.binance.com:9443/ws/{pair}@aggTrade")

    async def parse_message(self, raw: str | bytes) -> AsyncIterator[Trade]:
        """Parse a raw WebSocket frame into domain records."""
        data = json.loads(raw)
        if data.get("e") != "aggTrade":
            return
        yield Trade(
            ts=int(data["T"]) * 1_000_000,
            price=float(data["p"]),
            amount=float(data["q"]),
            side="sell" if data["m"] else "buy",
            tid=str(data["a"]),
        )


class _BinanceDepthWS(WebSocketBase):
    # Partial Book Depth Stream: a fully sorted top-N snapshot every 100ms
    # (``bids`` best-first, ``asks`` best-first). NOT ``@depth`` (the *diff*
    # stream), whose unsorted partial updates make best bid/ask meaningless and
    # often crossed.
    def __init__(self, pair: str, levels: int = 20) -> None:
        super().__init__(f"wss://stream.binance.com:9443/ws/{pair}@depth{levels}@100ms")

    async def parse_message(self, raw: str | bytes) -> AsyncIterator[OrderBookSnapshot]:
        """Parse a raw WebSocket frame into domain records."""
        import time
        data = json.loads(raw)
        if "bids" not in data or "asks" not in data:
            return
        bids = [OrderBookLevel(price=float(b[0]), amount=float(b[1])) for b in data["bids"]]
        asks = [OrderBookLevel(price=float(a[0]), amount=float(a[1])) for a in data["asks"]]
        yield OrderBookSnapshot(
            ts=s_to_ns(time.time()),
            bids=bids,
            asks=asks,
            is_snapshot=True,
        )

    async def stream(self, min_interval: float = 0.0) -> AsyncIterator[OrderBookSnapshot]:
        """Yield parsed order-book snapshots, throttled to *min_interval* seconds.

        The throttle check is applied on the raw frame **before** calling
        ``parse_message`` so no pydantic objects are constructed for frames that
        will be discarded.  ``min_interval=0.0`` preserves the legacy per-frame
        behaviour.
        """
        import time as _time
        last_emit: float = -float("inf")  # first frame always emits
        async for raw in self.stream_raw():
            # Fast path: do a cheap JSON sniff before committing to parse_message.
            now = _time.monotonic()
            if now - last_emit < min_interval:
                continue
            async for record in self.parse_message(raw):
                last_emit = _time.monotonic()
                yield record
