"""Bybit source adapter — OHLC full history, trades recent only (60), order book."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import AsyncIterator
from typing import Any

from dccd.domain.capability import Capability
from dccd.domain.records import OHLCBar, OrderBookLevel, OrderBookSnapshot, Trade
from dccd.domain.symbol import Symbol
from dccd.domain.timeutils import bybit_interval
from dccd.domain.types import DataType
from dccd.sources.base import (
    OHLCHistory,
    OHLCLive,
    OrderBookLive,
    OrderBookSnapshotREST,
    TradesLive,
)
from dccd.transport.http import AsyncHTTPClient
from dccd.transport.ws import WebSocketBase

__all__ = ["BybitSource"]

logger = logging.getLogger(__name__)

_BASE = "https://api.bybit.com/v5/market"


class BybitSource(OHLCHistory, OHLCLive, TradesLive, OrderBookSnapshotREST, OrderBookLive):
    """Bybit source adapter (spot).

    - **Backfill**: OHLC (full), order-book snapshot. **No trades** (see Notes).
    - **Stream**: OHLC, trades, order book.

    Notes
    -----
    Bybit spot exposes only the ~60 most recent trades and no history, so
    ``TradesHistory`` is deliberately **not implemented** — a trades backfill
    raises :class:`~dccd.domain.errors.NoCapability` rather than returning a
    misleading recent slice. Live trades are still available via the stream.

    See Also
    --------
    dccd.Client : the public facade.

    Examples
    --------
    >>> from dccd.sources.bybit import BybitSource
    >>> BybitSource().capability_for(DataType.TRADES, 'rest', 'historical') is None
    True
    """

    exchange = "bybit"

    def __init__(self, http: AsyncHTTPClient | None = None) -> None:
        self._http = http or AsyncHTTPClient()

    def capabilities(self) -> list[Capability]:
        """Declared capabilities, one per (data type × transport × mode)."""
        return [
            Capability(
                data_type=DataType.OHLC, transport="rest", mode="historical",
                history="full", max_per_request=1000, page_direction="forward",
                spans=[60, 180, 300, 900, 1800, 3600, 7200, 14400, 21600, 43200, 86400, 604800, 2592000],
            ),
            Capability(
                data_type=DataType.ORDERBOOK, transport="rest", mode="historical",
                max_depth=200,
            ),
            Capability(data_type=DataType.OHLC, transport="ws", mode="live"),
            Capability(data_type=DataType.TRADES, transport="ws", mode="live"),
            Capability(data_type=DataType.ORDERBOOK, transport="ws", mode="live", max_depth=200),
        ]

    def render_symbol(self, s: Symbol) -> str:
        """Render a canonical :class:`~dccd.domain.symbol.Symbol` to this exchange's string."""
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
        interval = bybit_interval(span)
        if not interval:
            return []
        params: dict[str, Any] = {
            "category": "spot",
            "symbol": self.render_symbol(symbol),
            "interval": interval,
            "start": start_ns // 1_000_000,
            "end": end_ns // 1_000_000,
            "limit": min(limit, 1000),
        }
        async with self._http as client:
            data = await client.get(f"{_BASE}/kline", params)

        if data.get("retCode") != 0:
            logger.error("Bybit kline error: %s", data.get("retMsg"))
            return []

        bars = []
        for e in data.get("result", {}).get("list", []):
            bars.append(OHLCBar(
                ts=int(e[0]) * 1_000_000,
                open=float(e[1]),
                high=float(e[2]),
                low=float(e[3]),
                close=float(e[4]),
                volume=float(e[5]),
                quote_volume=float(e[6]) if len(e) > 6 else None,
            ))
        return bars

    async def fetch_orderbook(self, symbol: Symbol, depth: int) -> OrderBookSnapshot:
        """Fetch a current order-book snapshot up to *depth* levels."""
        params = {"category": "spot", "symbol": self.render_symbol(symbol), "limit": min(depth, 200)}
        async with self._http as client:
            data = await client.get(f"{_BASE}/orderbook", params)

        book = data.get("result", {})
        bids = [OrderBookLevel(price=float(b[0]), amount=float(b[1])) for b in book.get("b", [])]
        asks = [OrderBookLevel(price=float(a[0]), amount=float(a[1])) for a in book.get("a", [])]
        ts_ms = book.get("ts", int(time.time() * 1000))
        return OrderBookSnapshot(ts=ts_ms * 1_000_000, bids=bids, asks=asks)

    def stream_ohlc(self, symbol: Symbol, span: int) -> AsyncIterator[OHLCBar]:
        """Stream live OHLC bars over WebSocket."""
        ws = _BybitWS(self.render_symbol(symbol), "kline", bybit_interval(span) or "1")
        return ws.stream_ohlc()

    def stream_trades(self, symbol: Symbol) -> AsyncIterator[Trade]:
        """Stream live trades over WebSocket."""
        ws = _BybitWS(self.render_symbol(symbol), "publicTrade")
        return ws.stream_trades()

    def stream_orderbook(self, symbol: Symbol, depth: int) -> AsyncIterator[OrderBookSnapshot]:
        """Stream live order-book snapshots/deltas over WebSocket."""
        ws = _BybitWS(self.render_symbol(symbol), "orderbook", str(depth))
        return ws.stream_orderbook()


class _BybitWS(WebSocketBase):
    def __init__(self, symbol: str, topic: str, param: str = "") -> None:
        super().__init__("wss://stream.bybit.com/v5/public/spot")
        self._symbol = symbol
        self._topic = topic
        self._param = param

    async def on_connect(self, ws: Any) -> None:
        """Send the subscription message after each (re)connect."""
        full_topic = f"{self._topic}.{self._param}.{self._symbol}" if self._param else f"{self._topic}.{self._symbol}"
        await ws.send(json.dumps({"op": "subscribe", "args": [full_topic]}))

    async def parse_message(self, raw: str | bytes) -> AsyncIterator[Any]:
        """Parse a raw WebSocket frame into domain records."""
        return
        yield

    async def stream_ohlc(self) -> AsyncIterator[OHLCBar]:
        """Stream live OHLC bars over WebSocket."""
        async for raw in self.stream_raw():
            data = json.loads(raw)
            if "data" not in data:
                continue
            for k in data["data"]:
                yield OHLCBar(
                    ts=int(k["start"]) * 1_000_000,
                    open=float(k["open"]),
                    high=float(k["high"]),
                    low=float(k["low"]),
                    close=float(k["close"]),
                    volume=float(k["volume"]),
                )

    async def stream_trades(self) -> AsyncIterator[Trade]:
        """Stream live trades over WebSocket."""
        async for raw in self.stream_raw():
            data = json.loads(raw)
            if "data" not in data:
                continue
            for t in data["data"]:
                yield Trade(
                    ts=int(t["T"]) * 1_000_000,
                    price=float(t["p"]),
                    amount=float(t["v"]),
                    side=t.get("S", "").lower() or None,
                    tid=t.get("i"),
                )

    async def stream_orderbook(self) -> AsyncIterator[OrderBookSnapshot]:
        """Stream live order-book snapshots/deltas over WebSocket."""
        async for raw in self.stream_raw():
            data = json.loads(raw)
            if "data" not in data:
                continue
            d = data["data"]
            is_snap = data.get("type") == "snapshot"
            bids = [OrderBookLevel(price=float(b[0]), amount=float(b[1])) for b in d.get("b", [])]
            asks = [OrderBookLevel(price=float(a[0]), amount=float(a[1])) for a in d.get("a", [])]
            ts_ms = d.get("ts", int(time.time() * 1000))
            yield OrderBookSnapshot(ts=ts_ms * 1_000_000, bids=bids, asks=asks, is_snapshot=is_snap)
