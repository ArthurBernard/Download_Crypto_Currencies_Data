"""Kraken source adapter — OHLC (720 recent), Trades full history, order book."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import AsyncIterator
from typing import Any

from dccd.domain.capability import Capability
from dccd.domain.records import OHLCBar, OrderBookLevel, OrderBookSnapshot, Trade
from dccd.domain.symbol import Symbol
from dccd.domain.timeutils import NS, kraken_interval, ns_to_s, s_to_ns
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

__all__ = ["KrakenSource"]

logger = logging.getLogger(__name__)

_BASE = "https://api.kraken.com/0/public"

_KRAKEN_PAIRS: dict[str, str] = {}


def _kraken_pair(symbol: Symbol) -> str:
    """Convert a Symbol to Kraken pair string (e.g. 'XBTUSD' or 'XXBTZUSD')."""
    base = symbol.base
    quote = symbol.quote
    if base == "BTC":
        base = "XBT"
    if base in ("BCH", "DASH"):
        return f"{base}{quote}"
    if quote in ("EUR", "USD", "CAD", "JPY", "GBP"):
        return f"X{base}Z{quote}"
    return f"X{base}X{quote}"


def _ws_pair(symbol: Symbol) -> str:
    """Kraken WS v2 format: BTC/USD."""
    base = symbol.base
    return f"{base}/{symbol.quote}"


class KrakenSource(
    OHLCHistory,
    TradesHistory,
    OrderBookSnapshotREST,
    OHLCLive,
    TradesLive,
    OrderBookLive,
):
    """Kraken adapter.

    OHLC REST: only the most recent 720 bars (history='recent').
    Trades REST: full history via since=0 cursor.
    OHLC deep history: NoCapability — use DerivedOHLCSource (M3).
    """

    exchange = "kraken"

    def __init__(self, http: AsyncHTTPClient | None = None) -> None:
        self._http = http or AsyncHTTPClient()

    def capabilities(self) -> list[Capability]:
        return [
            Capability(
                data_type=DataType.OHLC, transport="rest", mode="historical",
                history="recent", max_per_request=720, page_direction=None,
                spans=[60, 300, 900, 1800, 3600, 14400, 86400, 604800, 1296000],
            ),
            Capability(
                data_type=DataType.TRADES, transport="rest", mode="historical",
                history="full", max_per_request=1000, page_direction="forward",
            ),
            Capability(
                data_type=DataType.ORDERBOOK, transport="rest", mode="historical",
                max_depth=500,
            ),
            Capability(data_type=DataType.OHLC, transport="ws", mode="live"),
            Capability(data_type=DataType.TRADES, transport="ws", mode="live"),
            Capability(data_type=DataType.ORDERBOOK, transport="ws", mode="live"),
        ]

    def render_symbol(self, s: Symbol) -> str:
        return _kraken_pair(s)

    async def fetch_ohlc_page(
        self,
        symbol: Symbol,
        span: int,
        start_ns: int,
        end_ns: int,
        limit: int,
    ) -> list[OHLCBar]:
        minutes = kraken_interval(span)
        if not minutes:
            return []

        pair = _kraken_pair(symbol)
        params: dict[str, Any] = {
            "pair": pair,
            "interval": minutes,
            "since": int(ns_to_s(start_ns)) - span,
        }
        async with self._http as client:
            data = await client.get(f"{_BASE}/OHLC", params)

        if data.get("error"):
            logger.error("Kraken OHLC error: %s", data["error"])
            return []

        result_data = data.get("result", {})
        rows = result_data.get(pair, result_data.get(next(iter(result_data), ""), []))
        if not isinstance(rows, list):
            return []

        return [
            OHLCBar(
                ts=int(e[0]) * NS,
                open=float(e[1]),
                high=float(e[2]),
                low=float(e[3]),
                close=float(e[4]),
                volume=float(e[6]),
                quote_volume=float(e[6]) * float(e[5]) if e[5] else None,
            )
            for e in rows
        ]

    async def fetch_trades_page(
        self,
        symbol: Symbol,
        start_ns: int,
        end_ns: int,
        limit: int,
    ) -> list[Trade]:
        """Fetch trades using Kraken's cursor-based 'since' pagination."""
        pair = _kraken_pair(symbol)
        since_ns = start_ns

        all_trades: list[Trade] = []
        while True:
            params: dict[str, Any] = {
                "pair": pair,
                "since": str(since_ns),
            }
            async with self._http as client:
                data = await client.get(f"{_BASE}/Trades", params)

            if data.get("error"):
                logger.error("Kraken trades error: %s", data["error"])
                break

            result_data = data.get("result", {})
            raw_trades = result_data.get(pair, result_data.get(next(iter(k for k in result_data if k != "last"), ""), []))
            last_cursor = result_data.get("last")

            if not raw_trades:
                break

            for e in raw_trades:
                ts_ns = int(float(e[2]) * NS)
                if ts_ns > end_ns:
                    return all_trades
                all_trades.append(Trade(
                    ts=ts_ns,
                    price=float(e[0]),
                    amount=float(e[1]),
                    side="buy" if e[3] == "b" else "sell",
                    tid=None,
                ))

            if not last_cursor or len(raw_trades) < 1000:
                break
            since_ns = int(last_cursor)

        return all_trades

    async def fetch_orderbook(self, symbol: Symbol, depth: int) -> OrderBookSnapshot:
        pair = _kraken_pair(symbol)
        async with self._http as client:
            data = await client.get(f"{_BASE}/Depth", {"pair": pair, "count": min(depth, 500)})

        if data.get("error"):
            raise RuntimeError(f"Kraken depth error: {data['error']}")

        result = data.get("result", {})
        book = result.get(pair, next(iter(result.values()), {}))

        bids = [OrderBookLevel(price=float(b[0]), amount=float(b[1])) for b in book.get("bids", [])]
        asks = [OrderBookLevel(price=float(a[0]), amount=float(a[1])) for a in book.get("asks", [])]
        return OrderBookSnapshot(ts=s_to_ns(time.time()), bids=bids, asks=asks)

    def stream_ohlc(self, symbol: Symbol, span: int) -> AsyncIterator[OHLCBar]:
        ws = _KrakenWS(_ws_pair(symbol), "ohlc", span // 60)
        return ws.stream_ohlc()

    def stream_trades(self, symbol: Symbol) -> AsyncIterator[Trade]:
        ws = _KrakenWS(_ws_pair(symbol), "trade")
        return ws.stream_trades()

    def stream_orderbook(self, symbol: Symbol, depth: int) -> AsyncIterator[OrderBookSnapshot]:
        ws = _KrakenWS(_ws_pair(symbol), "book", depth)
        return ws.stream_orderbook()


class _KrakenWS(WebSocketBase):
    def __init__(self, pair: str, channel: str, param: int = 10) -> None:
        super().__init__("wss://ws.kraken.com/v2")
        self._pair = pair
        self._channel = channel
        self._param = param

    async def on_connect(self, ws: Any) -> None:
        sub: dict[str, Any] = {"method": "subscribe", "params": {"channel": self._channel, "symbol": [self._pair]}}
        if self._channel == "ohlc":
            sub["params"]["interval"] = self._param
        elif self._channel == "book":
            sub["params"]["depth"] = self._param
        await ws.send(json.dumps(sub))

    async def parse_message(self, raw: str | bytes) -> AsyncIterator[Any]:
        return
        yield

    async def stream_ohlc(self) -> AsyncIterator[OHLCBar]:
        async for raw in self._stream_raw():
            data = json.loads(raw)
            if data.get("channel") != "ohlc":
                continue
            for ohlc in data.get("data", []):
                yield OHLCBar(
                    ts=int(float(ohlc.get("timestamp_open", 0)) * NS),
                    open=float(ohlc.get("open", 0)),
                    high=float(ohlc.get("high", 0)),
                    low=float(ohlc.get("low", 0)),
                    close=float(ohlc.get("close", 0)),
                    volume=float(ohlc.get("volume", 0)),
                )

    async def stream_trades(self) -> AsyncIterator[Trade]:
        async for raw in self._stream_raw():
            data = json.loads(raw)
            if data.get("channel") != "trade":
                continue
            for t in data.get("data", []):
                ts_str = t.get("timestamp", "")
                try:
                    from datetime import datetime
                    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    ts_ns = s_to_ns(dt.timestamp())
                except Exception:
                    ts_ns = s_to_ns(time.time())
                yield Trade(
                    ts=ts_ns,
                    price=float(t.get("price", 0)),
                    amount=float(t.get("qty", 0)),
                    side="buy" if t.get("side") == "buy" else "sell",
                )

    async def stream_orderbook(self) -> AsyncIterator[OrderBookSnapshot]:
        state_bids: dict[float, float] = {}
        state_asks: dict[float, float] = {}
        async for raw in self._stream_raw():
            data = json.loads(raw)
            if data.get("channel") != "book":
                continue
            for snap in data.get("data", []):
                is_snap = snap.get("type") == "snapshot"
                bids_data = snap.get("bids", [])
                asks_data = snap.get("asks", [])
                if is_snap:
                    state_bids = {float(b["price"]): float(b["qty"]) for b in bids_data}
                    state_asks = {float(a["price"]): float(a["qty"]) for a in asks_data}
                else:
                    for b in bids_data:
                        p, q = float(b["price"]), float(b["qty"])
                        if q == 0:
                            state_bids.pop(p, None)
                        else:
                            state_bids[p] = q
                    for a in asks_data:
                        p, q = float(a["price"]), float(a["qty"])
                        if q == 0:
                            state_asks.pop(p, None)
                        else:
                            state_asks[p] = q

                bids = [OrderBookLevel(price=p, amount=q) for p, q in sorted(state_bids.items(), reverse=True)]
                asks = [OrderBookLevel(price=p, amount=q) for p, q in sorted(state_asks.items())]
                yield OrderBookSnapshot(ts=s_to_ns(time.time()), bids=bids, asks=asks, is_snapshot=is_snap)

    async def _stream_raw(self) -> AsyncIterator[str]:
        import asyncio

        import websockets
        while not self._stop.is_set():
            try:
                async with websockets.connect(self.url) as ws:
                    await self.on_connect(ws)
                    async for raw in ws:
                        if self._stop.is_set():
                            return
                        yield raw
            except asyncio.CancelledError:
                return
            except Exception as exc:
                if self._stop.is_set():
                    return
                logger.warning("Kraken WS error: %s — reconnecting", exc)
                import asyncio as _asyncio
                await _asyncio.sleep(5)
