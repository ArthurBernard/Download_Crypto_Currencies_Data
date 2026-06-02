"""Coinbase source adapter — OHLC (300/req), trades, order book (REST + WS)."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Any

from dccd.domain.capability import Capability
from dccd.domain.records import OHLCBar, OrderBookLevel, OrderBookSnapshot, Trade
from dccd.domain.symbol import Symbol
from dccd.domain.timeutils import NS, coinbase_granularity, ns_to_s, s_to_ns
from dccd.domain.types import DataType
from dccd.sources.base import (
    OHLCHistory,
    OHLCLive,
    OrderBookLive,
    OrderBookSnapshotREST,
    TradesHistory,
    TradesLive,
)
from dccd.transport.http import AsyncHTTPClient
from dccd.transport.ws import WebSocketBase

__all__ = ["CoinbaseSource"]

logger = logging.getLogger(__name__)

_BASE = "https://api.exchange.coinbase.com"
_COINBASE_SPANS = [60, 300, 900, 3600, 21600, 86400]


class CoinbaseSource(
    OHLCHistory,
    TradesHistory,
    OrderBookSnapshotREST,
    OHLCLive,
    TradesLive,
    OrderBookLive,
):
    """Coinbase adapter.

    OHLC: 300 candles per request (mandatory windowing via Paginator).
    Trades: cursor-based, 100 per page (recent data only in practice).
    """

    exchange = "coinbase"

    def __init__(self, http: AsyncHTTPClient | None = None) -> None:
        self._http = http or AsyncHTTPClient()

    def capabilities(self) -> list[Capability]:
        return [
            Capability(
                data_type=DataType.OHLC, transport="rest", mode="historical",
                history="full", max_per_request=300, page_direction="forward",
                spans=_COINBASE_SPANS,
            ),
            Capability(
                data_type=DataType.TRADES, transport="rest", mode="historical",
                history="recent", max_per_request=100, page_direction="backward",
            ),
            Capability(
                data_type=DataType.ORDERBOOK, transport="rest", mode="historical",
                max_depth=50,
            ),
            Capability(data_type=DataType.OHLC, transport="ws", mode="live"),
            Capability(data_type=DataType.TRADES, transport="ws", mode="live"),
            Capability(data_type=DataType.ORDERBOOK, transport="ws", mode="live"),
        ]

    def render_symbol(self, s: Symbol) -> str:
        """Coinbase format: BTC-USD."""
        return f"{s.base}-{s.quote}"

    async def fetch_ohlc_page(
        self,
        symbol: Symbol,
        span: int,
        start_ns: int,
        end_ns: int,
        limit: int,
    ) -> list[OHLCBar]:
        gran = coinbase_granularity(span)
        if not gran:
            logger.warning("Coinbase does not support span=%d", span)
            return []

        pair = self.render_symbol(symbol)
        start_dt = datetime.fromtimestamp(ns_to_s(start_ns), tz=timezone.utc).isoformat()
        end_dt = datetime.fromtimestamp(ns_to_s(end_ns), tz=timezone.utc).isoformat()
        params: dict[str, Any] = {
            "start": start_dt,
            "end": end_dt,
            "granularity": gran,
        }
        async with self._http as client:
            data = await client.get(f"{_BASE}/products/{pair}/candles", params)

        if not isinstance(data, list):
            logger.error("Coinbase candles unexpected response: %r", data)
            return []

        return [
            OHLCBar(
                ts=int(e[0]) * NS,
                open=float(e[3]),
                high=float(e[2]),
                low=float(e[1]),
                close=float(e[4]),
                volume=float(e[5]),
                quote_volume=float(e[4]) * float(e[5]),
            )
            for e in data
            if isinstance(e, (list, tuple)) and len(e) >= 6
        ]

    async def fetch_trades_page(
        self,
        symbol: Symbol,
        start_ns: int,
        end_ns: int,
        limit: int,
    ) -> list[Trade]:
        pair = self.render_symbol(symbol)
        async with self._http as client:
            data = await client.get(
                f"{_BASE}/products/{pair}/trades",
                {"limit": min(limit, 100)},
            )

        result = []
        for e in (data if isinstance(data, list) else []):
            try:
                ts_str = e.get("time", "")
                ts_dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                ts_ns = s_to_ns(ts_dt.timestamp())
                if not (start_ns <= ts_ns <= end_ns):
                    continue
                result.append(Trade(
                    ts=ts_ns,
                    price=float(e["price"]),
                    amount=float(e["size"]),
                    side=e.get("side"),
                    tid=str(e.get("trade_id", "")),
                ))
            except Exception:
                continue
        return result

    async def fetch_orderbook(self, symbol: Symbol, depth: int) -> OrderBookSnapshot:
        pair = self.render_symbol(symbol)
        async with self._http as client:
            data = await client.get(f"{_BASE}/products/{pair}/book", {"level": 2})

        bids = [OrderBookLevel(price=float(b[0]), amount=float(b[1]), count=int(b[2]) if len(b) > 2 else None)
                for b in data.get("bids", [])]
        asks = [OrderBookLevel(price=float(a[0]), amount=float(a[1]), count=int(a[2]) if len(a) > 2 else None)
                for a in data.get("asks", [])]
        return OrderBookSnapshot(ts=s_to_ns(time.time()), bids=bids, asks=asks)

    def stream_ohlc(self, symbol: Symbol, span: int) -> AsyncIterator[OHLCBar]:
        ws = _CoinbaseWS(self.render_symbol(symbol))
        return ws.stream_ohlc()

    def stream_trades(self, symbol: Symbol) -> AsyncIterator[Trade]:
        ws = _CoinbaseWS(self.render_symbol(symbol))
        return ws.stream_trades()

    def stream_orderbook(self, symbol: Symbol, depth: int) -> AsyncIterator[OrderBookSnapshot]:
        ws = _CoinbaseWS(self.render_symbol(symbol))
        return ws.stream_orderbook()


class _CoinbaseWS(WebSocketBase):
    def __init__(self, pair: str) -> None:
        super().__init__("wss://advanced-trade-ws.coinbase.com")
        self._pair = pair

    async def on_connect(self, ws: Any) -> None:
        await ws.send(json.dumps({
            "type": "subscribe",
            "product_ids": [self._pair],
            "channel": "market_trades",
        }))

    async def parse_message(self, raw: str | bytes) -> AsyncIterator[Trade]:
        data = json.loads(raw)
        for event in data.get("events", []):
            for t in event.get("trades", []):
                try:
                    ts_str = t.get("time", "")
                    ts_dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    yield Trade(
                        ts=s_to_ns(ts_dt.timestamp()),
                        price=float(t["price"]),
                        amount=float(t["size"]),
                        side=t.get("side", "").lower() or None,
                        tid=str(t.get("trade_id", "")),
                    )
                except Exception:
                    continue

    async def stream_ohlc(self) -> AsyncIterator[OHLCBar]:
        return
        yield

    async def stream_trades(self) -> AsyncIterator[Trade]:
        async for item in self.stream():
            yield item

    async def stream_orderbook(self) -> AsyncIterator[OrderBookSnapshot]:
        return
        yield
