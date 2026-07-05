"""Binance source adapter — OHLC, trades, order book (REST + WS)."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import AsyncIterator
from typing import Any

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
from dccd.domain.timeutils import binance_interval, s_to_ns
from dccd.domain.types import DataType
from dccd.sources.base import (
    FundingHistory,
    OHLCHistory,
    OHLCLive,
    OpenInterestHistory,
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
_BASE_FAPI = "https://fapi.binance.com/fapi/v1"
_BASE_FDATA = "https://fapi.binance.com/futures/data"
_CONTRACT_TYPE = {
    "perp": "PERPETUAL",
    "quarter": "CURRENT_QUARTER",
    "next_quarter": "NEXT_QUARTER",
}
# The only ``period`` values ``openInterestHist`` accepts — a strict subset of
# what :func:`~dccd.domain.timeutils.binance_interval` can emit (no 1m/3m/8h/…).
_OI_PERIODS = frozenset({"5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"})
# ``openInterestHist`` serves a rolling 30-day window and returns HTTP 400
# (code -1130, "parameter 'startTime' is invalid.") for any startTime at or
# beyond the boundary instead of trimming to it — verified 2026-07-04.
# The floor is therefore re-evaluated at request time, with a safety margin so
# clock skew, rate-limit waits and retries cannot push a request back over it.
_OI_WINDOW_MS = 30 * 86400 * 1000
_OI_FLOOR_MARGIN_MS = 5 * 60 * 1000


def _parse_ohlc_page(data: list[Any]) -> list[OHLCBar]:
    """Parse a raw Binance klines response into :class:`~dccd.domain.records.OHLCBar` records.

    Parameters
    ----------
    data:
        The parsed JSON list — each element is
        ``[open_time_ms, open, high, low, close, volume, close_time_ms,
        quote_vol, trades, ...]``.
    """
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


def _parse_aggtrades_page(data: list[Any], end_ms: int | None = None) -> tuple[list[Trade], str | None]:
    """Parse a raw Binance aggTrades response into :class:`~dccd.domain.records.Trade` records.

    Parameters
    ----------
    data:
        The parsed JSON list — each element is a dict with ``a`` (agg id),
        ``p`` (price), ``q`` (qty), ``T`` (timestamp ms), ``m`` (is maker).
    end_ms:
        Upper bound in milliseconds; used to decide whether to return a cursor.
    """
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
        if end_ms is not None and len(data) > 0 and last_ts_ms < end_ms
        else None
    )
    return trades, next_cursor


def _parse_funding_page(data: list[Any], limit: int) -> tuple[list[FundingRate], str | None]:
    """Parse a raw Binance ``fundingRate`` response into :class:`~dccd.domain.records.FundingRate` records.

    Parameters
    ----------
    data:
        The parsed JSON list — each element is a dict with ``fundingTime``
        (ms), ``fundingRate`` (str), and ``markPrice`` (str, may be empty or
        absent). The response is ascending by ``fundingTime``.
    limit:
        The *limit* passed to the request — used to detect a full page (the
        signal that more data may follow).
    """
    if not data:
        return [], None
    rates = [
        FundingRate(
            ts=int(e["fundingTime"]) * 1_000_000,
            rate=float(e["fundingRate"]),
            mark_price=float(e["markPrice"]) if e.get("markPrice") else None,
        )
        for e in data
    ]
    next_cursor = str(int(data[-1]["fundingTime"]) + 1) if len(data) == limit else None
    return rates, next_cursor


def _parse_oi_page(
    data: list[Any], span: int, end_ms: int,
) -> tuple[list[OpenInterest], str | None]:
    """Parse a raw Binance ``openInterestHist`` response into :class:`~dccd.domain.records.OpenInterest` records.

    Parameters
    ----------
    data:
        The parsed JSON list — each element is a dict with ``timestamp`` (ms),
        ``sumOpenInterest`` (str, base asset) and ``sumOpenInterestValue``
        (str, quote asset). The response is ascending by ``timestamp``.
    span:
        Observation cadence in seconds — the next cursor is the last
        timestamp advanced by one span.
    end_ms:
        The **global** window end in milliseconds. Because each request's
        window is bounded to the page capacity (see ``fetch_oi_page``), page
        fullness is *not* a reliable continuation signal — a boundary page can
        be short while plenty of window remains. The walk continues as long as
        the next slot still falls inside the global window.
    """
    if not data:
        return [], None
    oi = [
        OpenInterest(
            ts=int(e["timestamp"]) * 1_000_000,
            open_interest=float(e["sumOpenInterest"]),
            open_interest_value=float(e["sumOpenInterestValue"]),
        )
        for e in data
    ]
    next_ms = int(data[-1]["timestamp"]) + span * 1000
    next_cursor = str(next_ms) if next_ms <= end_ms else None
    return oi, next_cursor


class BinanceSource(
    OHLCHistory,
    TradesHistory,
    OrderBookSnapshotREST,
    FundingHistory,
    OpenInterestHistory,
    OHLCLive,
    TradesLive,
    OrderBookLive,
):
    """Binance source adapter (spot + USDS-M futures OHLC, funding, open interest).

    The reference adapter — full historical depth and every live channel.

    - **Backfill**: OHLC (``klines``, 1 000/req), trades (``aggTrades``,
      cursor-paginated by ``fromId``), order-book snapshot (``depth``, ≤ 5 000),
      realized funding rate (USDS-M ``fundingRate``, ``perp`` market only),
      open-interest statistics (USDS-M ``openInterestHist``, ``perp`` only —
      Binance serves at most the **last 30 days**, hence
      ``history="recent"`` + ``recent_window_s``; run a recurring job to
      accumulate history forward).
    - **Stream**: OHLC (``kline``), trades (``aggTrade``), order book (``depth``).
    - **Derivative OHLC**: a non-spot :attr:`~dccd.domain.symbol.Symbol.market`
      (``perp``, ``quarter``, ``next_quarter``) routes ``fetch_ohlc_page`` to
      the USDS-M futures ``continuousKlines`` endpoint instead of spot
      ``klines`` — no live futures channels are declared (WS caps stay
      spot-only).

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
    ['funding', 'ohlc', 'open_interest', 'orderbook', 'trades']
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
                markets=["spot", "perp", "quarter", "next_quarter"],
            ),
            Capability(
                data_type=DataType.TRADES, transport="rest", mode="historical",
                history="full", max_per_request=1000, page_direction="forward",
            ),
            Capability(
                data_type=DataType.ORDERBOOK, transport="rest", mode="historical",
                max_per_request=1, max_depth=5000,
            ),
            Capability(
                data_type=DataType.FUNDING, transport="rest", mode="historical",
                history="full", max_per_request=1000, page_direction="forward",
                markets=["perp"],
            ),
            Capability(
                data_type=DataType.OPEN_INTEREST, transport="rest", mode="historical",
                history="recent", recent_window_s=30 * 86400,
                max_per_request=500, page_direction="forward",
                markets=["perp"],
                spans=[300, 900, 1800, 3600, 7200, 14400, 21600, 43200, 86400],
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

        if symbol.market != "spot":
            # USDS-M futures (continuous contract): auto-rolled quarterly series
            # or perpetual OHLC. Same 12-field kline layout as spot, so
            # ``_parse_ohlc_page`` is reused unchanged. Uses the shared
            # "binance" rate-limit bucket (conservative — fapi actually has its
            # own, separate limits from spot) and the shared reference-counted
            # HTTP client.
            fapi_params: dict[str, Any] = {
                "pair": self.render_symbol(symbol),
                "contractType": _CONTRACT_TYPE[symbol.market],
                "interval": interval,
                "startTime": start_ns // 1_000_000,
                "endTime": end_ns // 1_000_000,
                "limit": min(limit, 1500),
            }
            async with self._http as client:
                data = await client.get(f"{_BASE_FAPI}/continuousKlines", fapi_params)
            return _parse_ohlc_page(data)

        params: dict[str, Any] = {
            "symbol": self.render_symbol(symbol),
            "interval": interval,
            "startTime": start_ns // 1_000_000,
            "endTime": end_ns // 1_000_000,
            "limit": limit,
        }
        async with self._http as client:
            data = await client.get(f"{_BASE_REST}/klines", params)

        return _parse_ohlc_page(data)

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

        return _parse_aggtrades_page(data, end_ms=end_ms if len(data) >= limit else None)

    async def fetch_funding_page(
        self,
        symbol: Symbol,
        start_ns: int,
        end_ns: int,
        limit: int,
        cursor: str | None = None,
    ) -> tuple[list[FundingRate], str | None]:
        """Fetch one page of realized funding rates (cursor = next ``startTime`` in ms).

        First call (``cursor=None``) is time-bounded on ``start_ns``; subsequent
        calls advance ``startTime`` to the last event's ``fundingTime`` + 1 ms.
        USDS-M futures only — ``fundingRate`` has no spot equivalent.
        """
        params: dict[str, Any] = {
            "symbol": self.render_symbol(symbol),
            "startTime": int(cursor) if cursor else start_ns // 1_000_000,
            "endTime": end_ns // 1_000_000,
            "limit": min(limit, 1000),
        }
        async with self._http as client:
            data = await client.get(f"{_BASE_FAPI}/fundingRate", params)

        return _parse_funding_page(data, min(limit, 1000))

    async def fetch_oi_page(
        self,
        symbol: Symbol,
        span: int,
        start_ns: int,
        end_ns: int,
        limit: int,
        cursor: str | None = None,
    ) -> tuple[list[OpenInterest], str | None]:
        """Fetch one page of open-interest statistics (cursor = next ``startTime`` in ms).

        USDS-M ``openInterestHist`` — ascending pages, walked forward like
        :meth:`fetch_funding_page`: the first call (``cursor=None``) anchors on
        ``start_ns``, then each page advances ``startTime`` to the last
        observation's timestamp + one *span*. Two quirks of the real endpoint
        (verified 2026-07-04) shape the request:

        - Binance serves only the **last 30 days** (declared as
          ``history="recent"``) and returns HTTP 400 for a ``startTime`` at or
          beyond the rolling boundary — so ``startTime`` is floored to the
          window (re-evaluated at request time, with a safety margin).
        - When the requested window holds more than ``limit`` observations,
          Binance returns the **latest** ones in it, not the earliest — so
          each request's ``endTime`` is bounded to the page capacity
          (``limit`` slots) and the continuation signal is *window left*, not
          page fullness.

        Returns ``([], None)`` without a request when *span* has no supported
        ``period`` mapping (only {5m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d}) or
        when the whole window predates the 30-day floor.
        """
        period = binance_interval(span)
        if period is None or period not in _OI_PERIODS:
            logger.warning("No Binance openInterestHist period for span=%d", span)
            return [], None

        limit_eff = min(limit, 500)
        end_ms = end_ns // 1_000_000
        start_ms = int(cursor) if cursor else start_ns // 1_000_000
        floor_ms = int(time.time() * 1000) - _OI_WINDOW_MS + _OI_FLOOR_MARGIN_MS
        start_ms = max(start_ms, floor_ms)
        if start_ms > end_ms:
            return [], None
        # ``limit`` slots from an *aligned* start span (limit-1)×span ms
        # inclusive; a wider window would drop its oldest observations.
        end_req_ms = min(end_ms, start_ms + (limit_eff - 1) * span * 1000)

        params: dict[str, Any] = {
            "symbol": self.render_symbol(symbol),
            "period": period,
            "limit": limit_eff,
            "startTime": start_ms,
            "endTime": end_req_ms,
        }
        async with self._http as client:
            data = await client.get(f"{_BASE_FDATA}/openInterestHist", params)

        return _parse_oi_page(data, span, end_ms)

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
