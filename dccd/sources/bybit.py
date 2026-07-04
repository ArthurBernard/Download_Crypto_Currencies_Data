"""Bybit source adapter — OHLC full history, trades recent only (60), order book, funding, open interest."""

from __future__ import annotations

# Built-in
import json
import logging
import time
from collections.abc import AsyncIterator
from typing import Any

# Local
from dccd.domain.capability import Capability
from dccd.domain.records import (
    FundingRate,
    OHLCBar,
    OpenInterest,
    OrderBookLevel,
    OrderBookSnapshot,
    Trade,
)
from dccd.domain.symbol import Symbol
from dccd.domain.timeutils import bybit_interval, bybit_oi_interval
from dccd.domain.types import DataType
from dccd.sources.base import (
    FundingHistory,
    OHLCHistory,
    OHLCLive,
    OpenInterestHistory,
    OrderBookLive,
    OrderBookSnapshotREST,
    TradesLive,
    default_http_client,
)
from dccd.transport.http import AsyncHTTPClient
from dccd.transport.ws import WebSocketBase

__all__ = ["BybitSource"]

logger = logging.getLogger(__name__)

_BASE = "https://api.bybit.com/v5/market"


class BybitSource(
    OHLCHistory, OHLCLive, TradesLive, OrderBookSnapshotREST, OrderBookLive,
    FundingHistory, OpenInterestHistory,
):
    """Bybit source adapter (spot + USDT-perpetual funding/open interest).

    - **Backfill**: OHLC (full), order-book snapshot, realized funding
      (``linear`` ``perp`` market only), open interest (``linear`` ``perp``,
      full history back to symbol launch). **No trades** (see Notes).
    - **Stream**: OHLC, trades, order book.

    Notes
    -----
    Bybit spot exposes only the ~60 most recent trades and no history, so
    ``TradesHistory`` is deliberately **not implemented** — a trades backfill
    raises :class:`~dccd.domain.errors.NoCapability` rather than returning a
    misleading recent slice. Live trades are still available via the stream.

    Funding history (``GET /v5/market/funding/history``) has two quirks:
    Bybit rejects a request carrying ``startTime`` without ``endTime`` (both
    must always be sent), and pages come back **newest-first** — the adapter
    walks backward, using each page's oldest timestamp minus one millisecond
    as the next page's ``endTime``.

    Open interest (``GET /v5/market/open-interest``) is span-typed (``5min``
    to ``1d`` buckets via :func:`~dccd.domain.timeutils.bybit_oi_interval`)
    and, unlike funding, exposes a **real** ``nextPageCursor`` — it is passed
    through unchanged as the opaque cursor instead of being reconstructed
    from timestamps.

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
        self._http = http or default_http_client(self.exchange)

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
            Capability(
                data_type=DataType.FUNDING, transport="rest", mode="historical",
                history="full", max_per_request=200, page_direction="backward",
                markets=["perp"],
            ),
            Capability(
                data_type=DataType.OPEN_INTEREST, transport="rest", mode="historical",
                history="full", max_per_request=200, page_direction="backward",
                markets=["perp"], spans=[300, 900, 1800, 3600, 14400, 86400],
            ),
            Capability(data_type=DataType.OHLC, transport="ws", mode="live"),
            Capability(data_type=DataType.TRADES, transport="ws", mode="live"),
            Capability(data_type=DataType.ORDERBOOK, transport="ws", mode="live", max_depth=1000, depths=[1, 50, 200, 1000]),
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

    async def fetch_funding_page(
        self,
        symbol: Symbol,
        start_ns: int,
        end_ns: int,
        limit: int,
        cursor: str | None = None,
    ) -> tuple[list[FundingRate], str | None]:
        """Fetch one page of realized funding rates, walking backward from ``end_ns``.

        Bybit's ``funding/history`` endpoint rejects a request that carries
        ``startTime`` without ``endTime`` — both are always sent. Pages are
        **newest-first**, so *cursor* (when set) is the previous page's oldest
        timestamp minus 1 ms, reused as this call's ``endTime``; ``startTime``
        stays pinned to ``start_ns`` throughout the walk. ``linear`` category
        only — this endpoint has no spot equivalent.
        """
        limit_eff = min(limit, 200)
        params: dict[str, Any] = {
            "category": "linear",
            "symbol": self.render_symbol(symbol),
            "startTime": start_ns // 1_000_000,
            "endTime": int(cursor) if cursor else end_ns // 1_000_000,
            "limit": limit_eff,
        }
        async with self._http as client:
            data = await client.get(f"{_BASE}/funding/history", params)

        if data.get("retCode") != 0:
            logger.error("Bybit funding error: %s", data.get("retMsg"))
            return [], None

        items = data.get("result", {}).get("list", [])
        if not items:
            return [], None

        rates = [
            FundingRate(
                ts=int(e["fundingRateTimestamp"]) * 1_000_000,
                rate=float(e["fundingRate"]),
            )
            for e in items
        ]

        oldest_ms = int(items[-1]["fundingRateTimestamp"])
        next_cursor = (
            str(oldest_ms - 1)
            if len(items) == limit_eff and oldest_ms > start_ns // 1_000_000
            else None
        )
        return rates, next_cursor

    async def fetch_oi_page(
        self,
        symbol: Symbol,
        span: int,
        start_ns: int,
        end_ns: int,
        limit: int,
        cursor: str | None = None,
    ) -> tuple[list[OpenInterest], str | None]:
        """Fetch one page of open interest, following Bybit's real cursor.

        Unlike ``funding/history``, ``open-interest`` returns a genuine
        ``result.nextPageCursor`` — it is passed through unchanged rather than
        reconstructed from timestamps. ``linear`` category only (no spot
        equivalent). Returns ``([], None)`` when *span* has no
        :func:`~dccd.domain.timeutils.bybit_oi_interval` mapping.
        """
        interval = bybit_oi_interval(span)
        if interval is None:
            return [], None

        params: dict[str, Any] = {
            "category": "linear",
            "symbol": self.render_symbol(symbol),
            "intervalTime": interval,
            "startTime": start_ns // 1_000_000,
            "endTime": end_ns // 1_000_000,
            "limit": min(limit, 200),
        }
        if cursor:
            params["cursor"] = cursor
        async with self._http as client:
            data = await client.get(f"{_BASE}/open-interest", params)

        if data.get("retCode") != 0:
            logger.error("Bybit open-interest error: %s", data.get("retMsg"))
            return [], None

        result = data.get("result", {})
        oi = [
            OpenInterest(
                ts=int(e["timestamp"]) * 1_000_000,
                open_interest=float(e["openInterest"]),
            )
            for e in result.get("list", [])
        ]
        next_cursor = result.get("nextPageCursor") or None
        return oi, next_cursor

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

    def stream_orderbook(
        self,
        symbol: Symbol,
        depth: int,
        *,
        min_interval: float = 0.0,
    ) -> AsyncIterator[OrderBookSnapshot]:
        """Stream live order-book snapshots/deltas over WebSocket."""
        ws = _BybitWS(self.render_symbol(symbol), "orderbook", str(depth))
        return ws.stream_orderbook(depth=depth, min_interval=min_interval)


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

    def _check_sub_ack(self, data: dict[str, Any]) -> None:
        """Raise on a rejected subscription instead of silently filtering it."""
        if data.get("op") == "subscribe" and data.get("success") is False:
            raise RuntimeError(
                f"bybit {self._topic} subscription rejected for "
                f"{self._symbol}: {data.get('ret_msg', 'unknown error')}"
            )

    async def parse_message(self, raw: str | bytes) -> AsyncIterator[Any]:
        """Parse a raw WebSocket frame into domain records."""
        return
        yield

    async def stream_ohlc(self) -> AsyncIterator[OHLCBar]:
        """Stream live OHLC bars over WebSocket."""
        async for raw in self.stream_raw():
            data = json.loads(raw)
            self._check_sub_ack(data)
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
            self._check_sub_ack(data)
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

    async def stream_orderbook(
        self, *, depth: int = 50, min_interval: float = 0.0
    ) -> AsyncIterator[OrderBookSnapshot]:
        """Stream the order book, reconstructing full state from snapshot + deltas.

        Bybit sends one ``snapshot`` then ``delta`` frames (a level with size 0
        is a removal). Delta frames are applied to cheap dict state on every WS
        frame; pydantic objects are only constructed when a capture is due
        (controlled by *min_interval*), eliminating per-frame CPU burn.

        At emit time both sides are sorted, truncated to *depth*, and the dicts
        are pruned to those same top-N levels. Bybit WS says the client truncates
        after applying updates; pruning at emit bounds stale-level retention to
        at most one interval.

        All emitted snapshots carry ``is_snapshot=True`` (full reconstructed state).
        """
        state_bids: dict[float, float] = {}
        state_asks: dict[float, float] = {}
        last_emit: float = -float("inf")  # ensure the first frame always emits
        async for raw in self.stream_raw():
            data = json.loads(raw)
            self._check_sub_ack(data)
            if "data" not in data:
                continue
            d = data["data"]
            is_snap = data.get("type") == "snapshot"
            if is_snap:
                state_bids = {float(b[0]): float(b[1]) for b in d.get("b", [])}
                state_asks = {float(a[0]): float(a[1]) for a in d.get("a", [])}
            else:
                for b in d.get("b", []):
                    price, qty = float(b[0]), float(b[1])
                    if qty == 0:
                        state_bids.pop(price, None)
                    else:
                        state_bids[price] = qty
                for a in d.get("a", []):
                    price, qty = float(a[0]), float(a[1])
                    if qty == 0:
                        state_asks.pop(price, None)
                    else:
                        state_asks[price] = qty
            # Throttle check: skip pydantic construction for frames that
            # won't be saved (min_interval=0.0 preserves legacy per-frame).
            now = time.monotonic()
            if now - last_emit < min_interval:
                continue
            last_emit = now
            # Sort + truncate to subscribed depth; prune dicts to match so
            # stale levels beyond depth are discarded at most one interval later.
            sorted_bids = sorted(state_bids.items(), reverse=True)[:depth]
            sorted_asks = sorted(state_asks.items())[:depth]
            state_bids = dict(sorted_bids)
            state_asks = dict(sorted_asks)
            bids = [OrderBookLevel(price=p, amount=q) for p, q in sorted_bids]
            asks = [OrderBookLevel(price=p, amount=q) for p, q in sorted_asks]
            ts_ms = d.get("ts", int(time.time() * 1000))
            yield OrderBookSnapshot(ts=ts_ms * 1_000_000, bids=bids, asks=asks, is_snapshot=True)
