"""BitMEX source adapter — bucketed OHLC (1m/5m/1h/1d), trades, order book."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import AsyncIterator
from typing import Any

from dccd.domain.capability import Capability
from dccd.domain.records import OHLCBar, OrderBookLevel, OrderBookSnapshot, Trade
from dccd.domain.symbol import Symbol
from dccd.domain.timeutils import NS, s_to_ns
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

__all__ = ["BitMEXSource"]

logger = logging.getLogger(__name__)

_BASE = "https://www.bitmex.com/api/v1"
_WS_URL = "wss://www.bitmex.com/realtime"

_BITMEX_BINS = {60: "1m", 300: "5m", 3600: "1h", 86400: "1d"}


class BitMEXSource(OHLCHistory, TradesHistory, OrderBookSnapshotREST, OHLCLive, TradesLive, OrderBookLive):
    """BitMEX source adapter.

    - **Backfill**: OHLC (bucketed — **1m / 5m / 1h / 1d only**), trades (full),
      order-book snapshot (``orderBook/L2``).
    - **Stream**: OHLC, trades, order book.

    Notes
    -----
    BitMEX buckets candles, so only the four spans above are available; other
    spans raise. BTC is rendered ``XBT`` (e.g. ``XBTUSD``).

    See Also
    --------
    dccd.Client : the public facade.

    Examples
    --------
    >>> from dccd.sources.bitmex import BitMEXSource
    >>> sorted(BitMEXSource().capability_for(DataType.OHLC, 'rest', 'historical').spans)
    [60, 300, 3600, 86400]
    """

    exchange = "bitmex"

    def __init__(self, http: AsyncHTTPClient | None = None) -> None:
        self._http = http or AsyncHTTPClient()

    def capabilities(self) -> list[Capability]:
        """Declared capabilities, one per (data type × transport × mode)."""
        return [
            Capability(
                data_type=DataType.OHLC, transport="rest", mode="historical",
                history="full", max_per_request=1000, page_direction="forward",
                spans=list(_BITMEX_BINS.keys()),
            ),
            Capability(
                data_type=DataType.TRADES, transport="rest", mode="historical",
                history="full", max_per_request=1000, page_direction="forward",
            ),
            Capability(data_type=DataType.ORDERBOOK, transport="rest", mode="historical"),
            Capability(data_type=DataType.OHLC, transport="ws", mode="live"),
            Capability(data_type=DataType.TRADES, transport="ws", mode="live"),
            Capability(data_type=DataType.ORDERBOOK, transport="ws", mode="live", max_depth=10),
        ]

    def render_symbol(self, s: Symbol) -> str:
        """Render a canonical :class:`~dccd.domain.symbol.Symbol` to this exchange's string."""
        base = "XBT" if s.base == "BTC" else s.base
        return f"{base}{s.quote}"

    async def fetch_ohlc_page(
        self,
        symbol: Symbol,
        span: int,
        start_ns: int,
        end_ns: int,
        limit: int,
    ) -> list[OHLCBar]:
        """Fetch one page of OHLC bars (see :meth:`~dccd.sources.base.OHLCHistory.fetch_ohlc_page`)."""
        bin_size = _BITMEX_BINS.get(span)
        if not bin_size:
            return []

        from datetime import datetime, timezone
        start_dt = datetime.fromtimestamp(start_ns / NS, tz=timezone.utc).isoformat()
        params: dict[str, Any] = {
            "symbol": self.render_symbol(symbol),
            "binSize": bin_size,
            "startTime": start_dt,
            "count": min(limit, 1000),
            "reverse": False,
        }
        async with self._http as client:
            data = await client.get(f"{_BASE}/trade/bucketed", params)

        bars = []
        for e in (data if isinstance(data, list) else []):
            from datetime import datetime, timezone
            ts_str = e.get("timestamp", "")
            try:
                dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                ts_ns = s_to_ns(dt.timestamp())
            except Exception:
                continue
            if ts_ns > end_ns:
                break
            bars.append(OHLCBar(
                ts=ts_ns,
                open=float(e.get("open") or 0),
                high=float(e.get("high") or 0),
                low=float(e.get("low") or 0),
                close=float(e.get("close") or 0),
                volume=float(e.get("volume") or 0),
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
        """Fetch one page of trades (cursor = ISO ``startTime``, forward).

        BitMEX returns up to 1 000 trades ascending from ``startTime``. On a
        full page we advance the cursor to the last timestamp + 1 ms to avoid
        re-fetching the boundary trade, and continue until the window is drained.
        """
        from datetime import datetime, timedelta, timezone
        page_count = min(limit, 1000)
        if cursor is not None:
            start_dt = cursor
        else:
            start_dt = datetime.fromtimestamp(start_ns / NS, tz=timezone.utc).isoformat()
        params: dict[str, Any] = {
            "symbol": self.render_symbol(symbol),
            "startTime": start_dt,
            "count": page_count,
            "reverse": False,
        }
        async with self._http as client:
            data = await client.get(f"{_BASE}/trade", params)

        rows = data if isinstance(data, list) else []
        trades: list[Trade] = []
        last_ts_ns: int | None = None
        for e in rows:
            ts_str = e.get("timestamp", "")
            try:
                dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                ts_ns = s_to_ns(dt.timestamp())
            except Exception:
                continue
            last_ts_ns = ts_ns
            trades.append(Trade(
                ts=ts_ns,
                price=float(e.get("price") or 0),
                amount=float(e.get("size") or 0),
                side="buy" if e.get("side") == "Buy" else "sell",
                tid=str(e.get("trdMatchID", "")),
            ))

        if len(rows) < page_count or last_ts_ns is None or last_ts_ns >= end_ns:
            return trades, None
        next_dt = datetime.fromtimestamp(last_ts_ns / NS, tz=timezone.utc) + timedelta(milliseconds=1)
        return trades, next_dt.isoformat()

    async def fetch_orderbook(self, symbol: Symbol, depth: int) -> OrderBookSnapshot:
        """Fetch a current order-book snapshot up to *depth* levels."""
        params = {"symbol": self.render_symbol(symbol), "depth": depth}
        async with self._http as client:
            data = await client.get(f"{_BASE}/orderBook/L2", params)

        bids, asks = [], []
        for e in (data if isinstance(data, list) else []):
            lvl = OrderBookLevel(price=float(e.get("price", 0)), amount=float(e.get("size", 0)))
            if e.get("side") == "Buy":
                bids.append(lvl)
            else:
                asks.append(lvl)

        return OrderBookSnapshot(ts=s_to_ns(time.time()), bids=bids, asks=asks)

    def stream_ohlc(self, symbol: Symbol, span: int) -> AsyncIterator[OHLCBar]:
        """Stream live OHLC bars over WebSocket."""
        bin_size = _BITMEX_BINS.get(span, "1m")
        ws = _BitMEXWS(self.render_symbol(symbol), f"tradeBin{bin_size}", "ohlc")
        return ws.stream()

    def stream_trades(self, symbol: Symbol) -> AsyncIterator[Trade]:
        """Stream live trades over WebSocket."""
        ws = _BitMEXWS(self.render_symbol(symbol), "trade", "trades")
        return ws.stream()

    def stream_orderbook(
        self,
        symbol: Symbol,
        depth: int,
        *,
        min_interval: float = 0.0,
    ) -> AsyncIterator[OrderBookSnapshot]:
        """Stream live order-book snapshots over WebSocket.

        Uses the ``orderBook10`` topic — a full top-10 snapshot on every update —
        rather than ``orderBookL2_25``, whose id-keyed insert/update/delete deltas
        carry no price on updates and can't yield a correct best bid-ask.
        """
        ws = _BitMEXWS(self.render_symbol(symbol), "orderBook10", "book")
        return ws.stream(min_interval=min_interval)


class _BitMEXWS(WebSocketBase):
    def __init__(self, symbol: str, topic: str, mode: str) -> None:
        super().__init__(_WS_URL)
        self._symbol = symbol
        self._topic = topic
        self._mode = mode

    async def on_connect(self, ws: Any) -> None:
        """Send the subscription message after each (re)connect."""
        await ws.send(json.dumps({"op": "subscribe", "args": [f"{self._topic}:{self._symbol}"]}))

    async def parse_message(self, raw: str | bytes) -> AsyncIterator[Any]:
        """Parse a raw WebSocket frame into domain records."""
        from datetime import datetime
        data = json.loads(raw)
        if "data" not in data:
            return

        if self._mode == "ohlc":
            for e in data["data"]:
                ts_str = e.get("timestamp", "")
                try:
                    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    ts_ns = s_to_ns(dt.timestamp())
                except Exception:
                    continue
                yield OHLCBar(
                    ts=ts_ns,
                    open=float(e.get("open") or 0),
                    high=float(e.get("high") or 0),
                    low=float(e.get("low") or 0),
                    close=float(e.get("close") or 0),
                    volume=float(e.get("volume") or 0),
                )

        elif self._mode == "trades":
            for e in data["data"]:
                ts_str = e.get("timestamp", "")
                try:
                    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                    ts_ns = s_to_ns(dt.timestamp())
                except Exception:
                    continue
                yield Trade(
                    ts=ts_ns,
                    price=float(e.get("price") or 0),
                    amount=float(e.get("size") or 0),
                    side="buy" if e.get("side") == "Buy" else "sell",
                    tid=str(e.get("trdMatchID", "")),
                )

        elif self._mode == "book":
            # orderBook10: each frame carries the full top-10 as `bids`/`asks`
            # arrays of [price, size] (bids best-first, asks best-first).
            for e in data["data"]:
                bids = [OrderBookLevel(price=float(b[0]), amount=float(b[1])) for b in e.get("bids", [])]
                asks = [OrderBookLevel(price=float(a[0]), amount=float(a[1])) for a in e.get("asks", [])]
                if bids or asks:
                    yield OrderBookSnapshot(ts=s_to_ns(time.time()), bids=bids, asks=asks, is_snapshot=True)

    async def stream(self, min_interval: float = 0.0) -> AsyncIterator[Any]:
        """Yield parsed records, with order-book frames throttled by *min_interval*.

        For order-book mode the throttle is applied on the raw frame **before**
        ``parse_message`` so no pydantic objects are constructed for frames that
        will be discarded.  For other modes behaves identically to the base
        ``stream()`` (min_interval is ignored).
        """
        if self._mode != "book" or min_interval == 0.0:
            async for record in super().stream():
                yield record
            return
        last_emit: float = -float("inf")  # first frame always emits
        async for raw in self.stream_raw():
            now = time.monotonic()
            if now - last_emit < min_interval:
                continue
            async for record in self.parse_message(raw):
                last_emit = time.monotonic()
                yield record
