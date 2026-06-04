"""Kraken source adapter — OHLC (720 recent), Trades full history, order book.

Kraken OHLC via REST is limited to the 720 most recent bars (``history="recent"``).
Full OHLC history must be derived from trades (M3, deferred).
Trades history is fully available via a cursor-based ``since`` parameter.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import AsyncIterator
from datetime import datetime
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


def _kraken_pair(symbol: Symbol) -> str:
    """Convert a canonical Symbol to a Kraken REST pair string.

    Examples
    --------
    >>> from dccd.domain.symbol import Symbol
    >>> _kraken_pair(Symbol(base='BTC', quote='USD'))
    'XXBTZUSD'
    """
    base = "XBT" if symbol.base == "BTC" else symbol.base
    if base in ("BCH", "DASH"):
        return f"{base}{symbol.quote}"
    if symbol.quote in ("EUR", "USD", "CAD", "JPY", "GBP"):
        return f"X{base}Z{symbol.quote}"
    return f"X{base}X{symbol.quote}"


def _ws_pair(symbol: Symbol) -> str:
    """Kraken WS v2 pair format (e.g. ``"BTC/USD"``)."""
    return f"{symbol.base}/{symbol.quote}"


def _parse_iso(ts_str: str, fallback: float | None = None) -> int:
    """Parse an ISO-8601 string to nanoseconds UTC."""
    try:
        dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        return s_to_ns(dt.timestamp())
    except Exception:
        return s_to_ns(fallback or time.time())


class KrakenSource(
    OHLCHistory,
    TradesHistory,
    OrderBookSnapshotREST,
    OHLCLive,
    TradesLive,
    OrderBookLive,
):
    """Kraken source adapter.

    - **Backfill**: OHLC (recent only — see Notes), trades (full, ``since``
      cursor), order-book snapshot.
    - **Stream**: OHLC, trades, order book (snapshot + deltas reconstructed).

    Notes
    -----
    Kraken's OHLC REST returns only the **720 most recent bars**
    (``history="recent"``); a deeper backfill is clamped to that window with a
    warning. Full deep OHLC would have to be derived from trades (deferred).

    See Also
    --------
    dccd.Client : the public facade.

    Examples
    --------
    >>> from dccd.sources.kraken import KrakenSource
    >>> KrakenSource().capability_for(DataType.OHLC, 'rest', 'historical').history
    'recent'
    """

    exchange = "kraken"

    def __init__(self, http: AsyncHTTPClient | None = None) -> None:
        self._http = http or AsyncHTTPClient()

    def capabilities(self) -> list[Capability]:
        """Declared capabilities, one per (data type × transport × mode)."""
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
            Capability(data_type=DataType.ORDERBOOK, transport="rest", mode="historical", max_depth=500),
            Capability(data_type=DataType.OHLC, transport="ws", mode="live"),
            Capability(data_type=DataType.TRADES, transport="ws", mode="live"),
            Capability(data_type=DataType.ORDERBOOK, transport="ws", mode="live"),
        ]

    def render_symbol(self, s: Symbol) -> str:
        """Render a canonical :class:`~dccd.domain.symbol.Symbol` to this exchange's string."""
        return _kraken_pair(s)

    async def fetch_ohlc_page(
        self, symbol: Symbol, span: int, start_ns: int, end_ns: int, limit: int,
    ) -> list[OHLCBar]:
        """Fetch up to *limit* OHLC bars from *start_ns*.

        .. warning::
            Kraken only returns the 720 most recent bars. Requests for deep
            history silently return recent data. Use the ``history="recent"``
            capability to inform the resolver.
        """
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
                # Kraken row: [time, o, h, l, c, vwap, volume, count]. VWAP =
                # quote_volume / base_volume, so vwap × volume is the exact quote
                # volume (not an approximation); count is the trade count.
                quote_volume=float(e[6]) * float(e[5]) if e[5] else None,
                trades=int(e[7]) if len(e) > 7 else None,
            )
            for e in rows
        ]

    async def fetch_trades_page(
        self, symbol: Symbol, start_ns: int, end_ns: int, limit: int,
        cursor: str | None = None,
    ) -> tuple[list[Trade], str | None]:
        """Fetch one page of trades (cursor = Kraken ``since`` ns).

        Kraken returns up to 1 000 trades from the ``since`` cursor and a
        ``result["last"]`` nanosecond cursor for the next page. We follow it
        until the page is short (caught up to the present) or the cursor stops
        advancing.
        """
        pair = _kraken_pair(symbol)
        since = cursor if cursor is not None else str(start_ns)
        params: dict[str, Any] = {"pair": pair, "since": since}
        async with self._http as client:
            data = await client.get(f"{_BASE}/Trades", params)

        if data.get("error"):
            logger.error("Kraken trades error: %s", data["error"])
            return [], None

        result_data = data.get("result", {})
        last = result_data.get("last")
        raw_trades = result_data.get(
            pair,
            result_data.get(next((k for k in result_data if k != "last"), ""), []),
        )

        trades: list[Trade] = [
            Trade(
                ts=int(float(e[2]) * NS),
                price=float(e[0]),
                amount=float(e[1]),
                side="buy" if e[3] == "b" else "sell",
                tid=None,
            )
            for e in raw_trades
        ]
        # Continue only while a full page came back and the cursor advanced.
        next_cursor = (
            str(last)
            if raw_trades and last is not None and str(last) != since and len(raw_trades) >= 1000
            else None
        )
        return trades, next_cursor

    async def fetch_orderbook(self, symbol: Symbol, depth: int) -> OrderBookSnapshot:
        """Fetch a current order-book snapshot up to *depth* levels."""
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
        """Stream live OHLC bars over WebSocket."""
        return _KrakenWS(_ws_pair(symbol), "ohlc", span // 60).stream_ohlc()

    def stream_trades(self, symbol: Symbol) -> AsyncIterator[Trade]:
        """Stream live trades over WebSocket."""
        return _KrakenWS(_ws_pair(symbol), "trade").stream_trades()

    def stream_orderbook(self, symbol: Symbol, depth: int) -> AsyncIterator[OrderBookSnapshot]:
        """Stream live order-book snapshots/deltas over WebSocket."""
        return _KrakenWS(_ws_pair(symbol), "book", depth).stream_orderbook()


class _KrakenWS(WebSocketBase):
    def __init__(self, pair: str, channel: str, param: int = 10) -> None:
        super().__init__("wss://ws.kraken.com/v2")
        self._pair = pair
        self._channel = channel
        self._param = param

    async def on_connect(self, ws: Any) -> None:
        """Send the subscription message after each (re)connect."""
        sub: dict[str, Any] = {
            "method": "subscribe",
            "params": {"channel": self._channel, "symbol": [self._pair]},
        }
        if self._channel == "ohlc":
            sub["params"]["interval"] = self._param
        elif self._channel == "book":
            sub["params"]["depth"] = self._param
        await ws.send(json.dumps(sub))

    async def stream_ohlc(self) -> AsyncIterator[OHLCBar]:
        """Stream live OHLC bars over WebSocket."""
        async for raw in self.stream_raw():
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
        """Stream live trades over WebSocket."""
        async for raw in self.stream_raw():
            data = json.loads(raw)
            if data.get("channel") != "trade":
                continue
            for t in data.get("data", []):
                yield Trade(
                    ts=_parse_iso(t.get("timestamp", ""), fallback=time.time()),
                    price=float(t.get("price", 0)),
                    amount=float(t.get("qty", 0)),
                    side="buy" if t.get("side") == "buy" else "sell",
                )

    async def stream_orderbook(self) -> AsyncIterator[OrderBookSnapshot]:
        """Reconstruct full order-book state from Kraken snapshot + delta frames."""
        state_bids: dict[float, float] = {}
        state_asks: dict[float, float] = {}
        async for raw in self.stream_raw():
            data = json.loads(raw)
            if data.get("channel") != "book":
                continue
            for snap in data.get("data", []):
                is_snap = snap.get("type") == "snapshot"
                if is_snap:
                    state_bids = {float(b["price"]): float(b["qty"]) for b in snap.get("bids", [])}
                    state_asks = {float(a["price"]): float(a["qty"]) for a in snap.get("asks", [])}
                else:
                    for b in snap.get("bids", []):
                        p, q = float(b["price"]), float(b["qty"])
                        if q == 0:
                            state_bids.pop(p, None)
                        else:
                            state_bids[p] = q
                    for a in snap.get("asks", []):
                        p, q = float(a["price"]), float(a["qty"])
                        if q == 0:
                            state_asks.pop(p, None)
                        else:
                            state_asks[p] = q
                bids = [OrderBookLevel(price=p, amount=q)
                        for p, q in sorted(state_bids.items(), reverse=True)]
                asks = [OrderBookLevel(price=p, amount=q)
                        for p, q in sorted(state_asks.items())]
                yield OrderBookSnapshot(ts=s_to_ns(time.time()), bids=bids, asks=asks, is_snapshot=is_snap)
