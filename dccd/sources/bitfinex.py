"""Bitfinex source adapter — OHLC (10000/req), trades, order book."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import AsyncIterator
from typing import Any

from dccd.domain.capability import Capability
from dccd.domain.records import OHLCBar, OrderBookLevel, OrderBookSnapshot, Trade
from dccd.domain.symbol import Symbol
from dccd.domain.timeutils import s_to_ns
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

__all__ = ["BitfinexSource"]

logger = logging.getLogger(__name__)

_BASE = "https://api-pub.bitfinex.com/v2"
_WS_URL = "wss://api-pub.bitfinex.com/ws/2"

_BITFINEX_SPANS = {
    60: "1m", 300: "5m", 900: "15m", 1800: "30m",
    3600: "1h", 10800: "3h", 21600: "6h", 43200: "12h",
    86400: "1D", 604800: "1W", 1209600: "14D", 2592000: "1M",
}


def _bfx_symbol(s: Symbol) -> str:
    """Bitfinex trading pair format, e.g. ``tBTCUSD``.

    Bitfinex labels Tether as ``UST`` (not ``USDT``), so ``BTC/USDT`` must map to
    ``tBTCUST`` — ``tBTCUSDT`` returns an empty list (HTTP 200), which would
    otherwise look like "0 rows" with no error. Symbols whose base or quote is
    longer than 3 characters use the ``tBASE:QUOTE`` form.
    """
    quote = "UST" if s.quote == "USDT" else s.quote
    if len(s.base) > 3 or len(quote) > 3:
        return f"t{s.base}:{quote}"
    return f"t{s.base}{quote}"


class BitfinexSource(OHLCHistory, TradesHistory, OrderBookSnapshotREST, OHLCLive, TradesLive, OrderBookLive):
    """Bitfinex adapter — 10000 bars/trades per request (largest limit)."""

    exchange = "bitfinex"

    def __init__(self, http: AsyncHTTPClient | None = None) -> None:
        self._http = http or AsyncHTTPClient()

    def capabilities(self) -> list[Capability]:
        return [
            Capability(
                data_type=DataType.OHLC, transport="rest", mode="historical",
                history="full", max_per_request=10000, page_direction="forward",
                spans=list(_BITFINEX_SPANS.keys()),
            ),
            Capability(
                data_type=DataType.TRADES, transport="rest", mode="historical",
                history="full", max_per_request=10000, page_direction="backward",
            ),
            Capability(
                data_type=DataType.ORDERBOOK, transport="rest", mode="historical",
                max_depth=250,
            ),
            Capability(data_type=DataType.OHLC, transport="ws", mode="live"),
            Capability(data_type=DataType.TRADES, transport="ws", mode="live"),
            # Order-book WS is not parsed yet (the book channel is unhandled) —
            # not declared so the engine rejects it rather than streaming empty.
        ]

    def render_symbol(self, s: Symbol) -> str:
        return _bfx_symbol(s)

    async def fetch_ohlc_page(
        self,
        symbol: Symbol,
        span: int,
        start_ns: int,
        end_ns: int,
        limit: int,
    ) -> list[OHLCBar]:
        tf = _BITFINEX_SPANS.get(span)
        if not tf:
            return []
        sym = _bfx_symbol(symbol)
        params: dict[str, Any] = {
            "start": start_ns // 1_000_000,
            "end": end_ns // 1_000_000,
            "limit": min(limit, 10000),
            "sort": 1,
        }
        async with self._http as client:
            data = await client.get(f"{_BASE}/candles/trade:{tf}:{sym}/hist", params)

        return [
            OHLCBar(
                ts=int(e[0]) * 1_000_000,
                open=float(e[1]),
                close=float(e[2]),
                high=float(e[3]),
                low=float(e[4]),
                volume=float(e[5]),
            )
            for e in (data if isinstance(data, list) else [])
            if isinstance(e, list) and len(e) >= 6
        ]

    async def fetch_trades_page(
        self,
        symbol: Symbol,
        start_ns: int,
        end_ns: int,
        limit: int,
        cursor: str | None = None,
    ) -> tuple[list[Trade], str | None]:
        """Fetch one page of trades (cursor = ``start`` ts in ms, forward).

        Bitfinex returns up to 10 000 trades ascending in ``[start, end]``. When
        a full page comes back the window held more than one page, so we advance
        the cursor to the last timestamp + 1 ms and continue.
        """
        sym = _bfx_symbol(symbol)
        page_limit = min(limit, 10000)
        start_ms = int(cursor) if cursor is not None else start_ns // 1_000_000
        params: dict[str, Any] = {
            "start": start_ms,
            "end": end_ns // 1_000_000,
            "limit": page_limit,
            "sort": 1,
        }
        async with self._http as client:
            data = await client.get(f"{_BASE}/trades/{sym}/hist", params)

        rows = [e for e in (data if isinstance(data, list) else [])
                if isinstance(e, list) and len(e) >= 4]
        trades = [
            Trade(
                ts=int(e[1]) * 1_000_000,
                price=float(e[3]),
                amount=abs(float(e[2])),
                side="buy" if float(e[2]) > 0 else "sell",
                tid=str(e[0]),
            )
            for e in rows
        ]
        if len(rows) < page_limit:
            return trades, None
        next_cursor = str(int(rows[-1][1]) + 1)
        return trades, next_cursor

    async def fetch_orderbook(self, symbol: Symbol, depth: int) -> OrderBookSnapshot:
        sym = _bfx_symbol(symbol)
        params = {"len": min(depth, 250)}
        async with self._http as client:
            data = await client.get(f"{_BASE}/book/{sym}/P0", params)

        bids, asks = [], []
        for e in (data if isinstance(data, list) else []):
            if not isinstance(e, list) or len(e) < 3:
                continue
            price, count, amount = float(e[0]), int(e[1]), float(e[2])
            if amount > 0:
                bids.append(OrderBookLevel(price=price, amount=amount, count=count))
            else:
                asks.append(OrderBookLevel(price=price, amount=abs(amount), count=count))

        return OrderBookSnapshot(
            ts=s_to_ns(time.time()),
            bids=sorted(bids, key=lambda x: -x.price),
            asks=sorted(asks, key=lambda x: x.price),
        )

    def stream_ohlc(self, symbol: Symbol, span: int) -> AsyncIterator[OHLCBar]:
        ws = _BitfinexWS(_bfx_symbol(symbol), "candles", _BITFINEX_SPANS.get(span, "1m"))
        return ws.stream()

    def stream_trades(self, symbol: Symbol) -> AsyncIterator[Trade]:
        ws = _BitfinexWS(_bfx_symbol(symbol), "trades")
        return ws.stream()

    def stream_orderbook(self, symbol: Symbol, depth: int) -> AsyncIterator[OrderBookSnapshot]:
        raise NotImplementedError("Bitfinex live order book stream is not implemented")


class _BitfinexWS(WebSocketBase):
    def __init__(self, symbol: str, channel: str, tf: str = "") -> None:
        super().__init__(_WS_URL)
        self._symbol = symbol
        self._channel = channel
        self._tf = tf
        self._chan_id: int | None = None

    async def on_connect(self, ws: Any) -> None:
        sub: dict[str, Any] = {"event": "subscribe", "channel": self._channel, "symbol": self._symbol}
        if self._channel == "candles":
            sub["key"] = f"trade:{self._tf}:{self._symbol}"
        await ws.send(json.dumps(sub))

    async def parse_message(self, raw: str | bytes) -> AsyncIterator[Any]:
        data = json.loads(raw)
        if isinstance(data, dict):
            if data.get("event") == "subscribed":
                self._chan_id = data.get("chanId")
            return

        if not isinstance(data, list) or len(data) < 2:
            return

        if data[0] != self._chan_id:
            return

        payload = data[1]
        if payload == "hb":
            return

        if self._channel == "trades" and isinstance(payload, list) and len(payload) >= 4:
            ts_ns = int(payload[1]) * 1_000_000
            yield Trade(
                ts=ts_ns,
                price=float(payload[3]),
                amount=abs(float(payload[2])),
                side="buy" if float(payload[2]) > 0 else "sell",
                tid=str(payload[0]),
            )
        elif self._channel == "candles" and isinstance(payload, list):
            items = payload if isinstance(payload[0], list) else [payload]
            for e in items:
                if len(e) >= 6:
                    yield OHLCBar(
                        ts=int(e[0]) * 1_000_000,
                        open=float(e[1]),
                        close=float(e[2]),
                        high=float(e[3]),
                        low=float(e[4]),
                        volume=float(e[5]),
                    )
