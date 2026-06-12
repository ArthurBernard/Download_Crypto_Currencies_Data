"""OKX source adapter — OHLC (candles + history-candles), trades, order book."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import AsyncIterator
from typing import Any

from dccd.domain.capability import Capability
from dccd.domain.records import OHLCBar, OrderBookLevel, OrderBookSnapshot, Trade
from dccd.domain.symbol import Symbol
from dccd.domain.timeutils import okx_interval
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

__all__ = ["OKXSource"]

logger = logging.getLogger(__name__)

_BASE = "https://www.okx.com/api/v5/market"
_WS_BASE = "wss://ws.okx.com:8443/ws/v5/public"


class OKXSource(
    OHLCHistory,
    TradesHistory,
    OrderBookSnapshotREST,
    OHLCLive,
    TradesLive,
    OrderBookLive,
):
    """OKX source adapter.

    - **Backfill**: OHLC (full, via ``history-candles``), trades (full, via
      ``history-trades``, paged backward by timestamp), order-book snapshot.
    - **Stream**: OHLC, trades, order book.

    See Also
    --------
    dccd.Client : the public facade.

    Examples
    --------
    >>> from dccd.sources.okx import OKXSource
    >>> OKXSource().capability_for(DataType.TRADES, 'rest', 'historical').history
    'full'
    """

    exchange = "okx"

    def __init__(self, http: AsyncHTTPClient | None = None) -> None:
        self._http = http or default_http_client(self.exchange)

    def capabilities(self) -> list[Capability]:
        """Declared capabilities, one per (data type × transport × mode)."""
        return [
            Capability(
                data_type=DataType.OHLC, transport="rest", mode="historical",
                history="full", max_per_request=100, page_direction="backward",
                spans=[60, 180, 300, 900, 1800, 3600, 7200, 14400, 21600, 43200, 86400, 604800, 2592000],
            ),
            Capability(
                data_type=DataType.TRADES, transport="rest", mode="historical",
                history="full", max_per_request=100, page_direction="backward",
            ),
            Capability(
                data_type=DataType.ORDERBOOK, transport="rest", mode="historical",
                max_depth=400,
            ),
            Capability(data_type=DataType.OHLC, transport="ws", mode="live"),
            Capability(data_type=DataType.TRADES, transport="ws", mode="live"),
            Capability(data_type=DataType.ORDERBOOK, transport="ws", mode="live", max_depth=5, depths=[5]),
        ]

    def render_symbol(self, s: Symbol) -> str:
        """Render a canonical :class:`~dccd.domain.symbol.Symbol` to this exchange's string."""
        return f"{s.base}-{s.quote}"

    async def fetch_ohlc_page(
        self,
        symbol: Symbol,
        span: int,
        start_ns: int,
        end_ns: int,
        limit: int,
    ) -> list[OHLCBar]:
        """Fetch one page of OHLC bars (see :meth:`~dccd.sources.base.OHLCHistory.fetch_ohlc_page`)."""
        bar = okx_interval(span)
        if not bar:
            return []

        pair = self.render_symbol(symbol)
        # OKX `before`/`after` are exclusive bounds. Without `- 1` the bar
        # exactly at each page-window start would be silently dropped.
        params: dict[str, Any] = {
            "instId": pair,
            "bar": bar,
            "before": str(start_ns // 1_000_000 - 1),
            "after": str(end_ns // 1_000_000),
            "limit": min(limit, 100),
        }
        async with self._http as client:
            data = await client.get(f"{_BASE}/history-candles", params)

        bars = []
        for e in data.get("data", []):
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

    async def fetch_trades_page(
        self,
        symbol: Symbol,
        start_ns: int,
        end_ns: int,
        limit: int,
        cursor: str | None = None,
    ) -> tuple[list[Trade], str | None]:
        """Fetch one page of trades (cursor = OKX ``after`` ts in ms).

        OKX ``history-trades`` (type=2) returns records *earlier than* the
        ``after`` timestamp, newest first. We page **backward** from ``end_ns``
        until the oldest item drops below ``start_ns`` or a short page arrives.
        """
        pair = self.render_symbol(symbol)
        after = cursor if cursor is not None else str(end_ns // 1_000_000)
        params: dict[str, Any] = {
            "instId": pair,
            "limit": min(limit, 100),
            "after": after,
            "type": "2",
        }
        async with self._http as client:
            data = await client.get(f"{_BASE}/history-trades", params)

        rows = data.get("data", [])
        trades = [
            Trade(
                ts=int(e["ts"]) * 1_000_000,
                price=float(e["px"]),
                amount=float(e["sz"]),
                side="buy" if e.get("side") == "buy" else "sell",
                tid=str(e.get("tradeId", "")),
            )
            for e in rows
        ]
        if not rows or len(rows) < min(limit, 100):
            return trades, None
        oldest_ts_ms = int(rows[-1]["ts"])
        next_cursor = str(oldest_ts_ms) if oldest_ts_ms > start_ns // 1_000_000 else None
        return trades, next_cursor

    async def fetch_orderbook(self, symbol: Symbol, depth: int) -> OrderBookSnapshot:
        """Fetch a current order-book snapshot up to *depth* levels."""
        pair = self.render_symbol(symbol)
        params = {"instId": pair, "sz": min(depth, 400)}
        async with self._http as client:
            data = await client.get(f"{_BASE}/books", params)

        book = (data.get("data") or [{}])[0]
        bids = [OrderBookLevel(price=float(b[0]), amount=float(b[1])) for b in book.get("bids", [])]
        asks = [OrderBookLevel(price=float(a[0]), amount=float(a[1])) for a in book.get("asks", [])]
        ts_ms = int(book.get("ts", int(time.time() * 1000)))
        return OrderBookSnapshot(ts=ts_ms * 1_000_000, bids=bids, asks=asks)

    def stream_ohlc(self, symbol: Symbol, span: int) -> AsyncIterator[OHLCBar]:
        """Stream live OHLC bars over WebSocket."""
        bar = okx_interval(span) or "1m"
        ws = _OKXWS(self.render_symbol(symbol), "candle" + bar, "ohlc")
        return ws.stream()

    def stream_trades(self, symbol: Symbol) -> AsyncIterator[Trade]:
        """Stream live trades over WebSocket."""
        ws = _OKXWS(self.render_symbol(symbol), "trades", "trades")
        return ws.stream()

    def stream_orderbook(
        self,
        symbol: Symbol,
        depth: int,
        *,
        min_interval: float = 0.0,
    ) -> AsyncIterator[OrderBookSnapshot]:
        """Stream live order-book snapshots over WebSocket.

        Uses the ``books5`` channel — a full sorted top-5 snapshot pushed every
        100ms — rather than ``books``, whose unmerged deltas yield a
        meaningless/crossed best bid-ask.
        """
        ws = _OKXWS(self.render_symbol(symbol), "books5", "books")
        return ws.stream(min_interval=min_interval)


class _OKXWS(WebSocketBase):
    def __init__(self, instId: str, channel: str, mode: str) -> None:
        super().__init__(_WS_BASE)
        self._instId = instId
        self._channel = channel
        self._mode = mode

    async def on_connect(self, ws: Any) -> None:
        """Send the subscription message after each (re)connect."""
        await ws.send(json.dumps({
            "op": "subscribe",
            "args": [{"channel": self._channel, "instId": self._instId}],
        }))

    def _check_sub_ack(self, data: dict[str, Any]) -> None:
        """Raise on a rejected subscription instead of silently filtering it."""
        if data.get("event") == "error":
            raise RuntimeError(
                f"okx {self._channel} subscription rejected for "
                f"{self._instId}: {data.get('msg', 'unknown error')} "
                f"(code {data.get('code', '?')})"
            )

    async def parse_message(self, raw: str | bytes) -> AsyncIterator[Any]:
        """Parse a raw WebSocket frame into domain records."""
        data = json.loads(raw)
        self._check_sub_ack(data)
        if "data" not in data:
            return
        if self._mode == "ohlc":
            for e in data["data"]:
                yield OHLCBar(
                    ts=int(e[0]) * 1_000_000,
                    open=float(e[1]),
                    high=float(e[2]),
                    low=float(e[3]),
                    close=float(e[4]),
                    volume=float(e[5]),
                )
        elif self._mode == "trades":
            for e in data["data"]:
                yield Trade(
                    ts=int(e["ts"]) * 1_000_000,
                    price=float(e["px"]),
                    amount=float(e["sz"]),
                    side=e.get("side"),
                    tid=str(e.get("tradeId", "")),
                )
        elif self._mode == "books":
            for snap in data["data"]:
                bids = [OrderBookLevel(price=float(b[0]), amount=float(b[1])) for b in snap.get("bids", [])]
                asks = [OrderBookLevel(price=float(a[0]), amount=float(a[1])) for a in snap.get("asks", [])]
                ts_ms = int(snap.get("ts", int(time.time() * 1000)))
                yield OrderBookSnapshot(ts=ts_ms * 1_000_000, bids=bids, asks=asks)

    async def stream(self, min_interval: float = 0.0) -> AsyncIterator[Any]:
        """Yield parsed records, with order-book frames throttled by *min_interval*.

        For order-book mode the throttle is applied on the raw frame **before**
        ``parse_message`` so no pydantic objects are constructed for frames that
        will be discarded.  For other modes behaves identically to the base
        ``stream()`` (min_interval is ignored).
        """
        if self._mode != "books" or min_interval == 0.0:
            # Non-books modes and the legacy zero-interval path use the base loop.
            async for record in super().stream():
                yield record
            return
        last_emit: float = -float("inf")  # first frame always emits
        async for raw in self.stream_raw():
            now = time.monotonic()
            if now - last_emit < min_interval:
                # Throttled frames are still checked for a subscription
                # rejection — swallowing it here would leave a silent stream.
                self._check_sub_ack(json.loads(raw))
                continue
            async for record in self.parse_message(raw):
                last_emit = time.monotonic()
                yield record
