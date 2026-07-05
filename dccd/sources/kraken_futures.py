"""Kraken Futures source adapter — hourly perp funding history + perp klines."""

from __future__ import annotations

# Built-in
import logging
from datetime import datetime
from typing import Any

# Local
from dccd.domain.capability import Capability
from dccd.domain.records import FundingRate, OHLCBar
from dccd.domain.symbol import Symbol
from dccd.domain.timeutils import NS, kraken_futures_resolution
from dccd.domain.types import DataType
from dccd.sources.base import FundingHistory, OHLCHistory, default_http_client
from dccd.transport.http import AsyncHTTPClient

__all__ = ["KrakenFuturesSource"]

logger = logging.getLogger(__name__)

_BASE = "https://futures.kraken.com/derivatives/api"
_BASE_CHARTS = "https://futures.kraken.com/api/charts/v1"

# Kraken names Bitcoin XBT on both spot and futures; canonical Symbols carry
# BTC (Symbol.parse normalises the other direction), so alias at render time.
_KRAKEN_ALIASES = {"BTC": "XBT"}


class KrakenFuturesSource(FundingHistory, OHLCHistory):
    """Kraken Futures source adapter (linear ``PF_`` perpetuals: funding + klines).

    Kraken Futures is a **separate API surface** from spot Kraken — different
    host (``futures.kraken.com``), different symbology (``PF_XBTUSD``) and
    different JSON shapes — so it gets its own adapter and exchange name
    (``krakenfutures``) rather than extra methods on
    :class:`~dccd.sources.kraken.KrakenSource`.

    - **Funding backfill**: realized funding rates (``perp`` market only), at
      Kraken's **1-hour cadence** — denser than the ~8h cadence of
      Binance/Bybit; normalise before any cross-exchange comparison. The
      endpoint serves a hard **~1-year rolling window** in a single
      unpaginated response (``history="recent"`` + ``recent_window_s``); run
      a recurring job to accumulate history forward.
    - **OHLC backfill**: perp klines via the **charts API**
      (``/api/charts/v1/trade/{symbol}/{resolution}``) — deep history
      (``history="full"``), ~2 000 candles per page, forward-paged,
      9 resolutions (1m…1w).

    No WebSocket channels or open interest are declared — Kraken Futures has
    no OI *history* endpoint (snapshot only), and undeclared capabilities
    must stay undeclared (honesty invariant).

    See Also
    --------
    dccd.Client : the public facade.
    dccd.sources.kraken.KrakenSource : the spot adapter (separate API).

    Examples
    --------
    >>> from dccd.sources.kraken_futures import KrakenFuturesSource
    >>> sorted({c.data_type.value for c in KrakenFuturesSource().capabilities()})
    ['funding', 'ohlc']
    """

    exchange = "krakenfutures"

    def __init__(self, http: AsyncHTTPClient | None = None) -> None:
        self._http = http or default_http_client(self.exchange)

    def capabilities(self) -> list[Capability]:
        """Declared capabilities, one per (data type × transport × mode)."""
        return [
            Capability(
                data_type=DataType.FUNDING, transport="rest", mode="historical",
                history="recent", recent_window_s=365 * 86400,
                max_per_request=10000, page_direction=None,
                markets=["perp"],
            ),
            Capability(
                data_type=DataType.OHLC, transport="rest", mode="historical",
                history="full", max_per_request=2000, page_direction="forward",
                spans=[60, 300, 900, 1800, 3600, 14400, 43200, 86400, 604800],
                markets=["perp"],
            ),
        ]

    def render_symbol(self, s: Symbol) -> str:
        """Kraken Futures linear-perp format: ``PF_XBTUSD`` (BTC aliased to XBT)."""
        base = _KRAKEN_ALIASES.get(s.base, s.base)
        return f"PF_{base}{s.quote}"

    async def fetch_funding_page(
        self,
        symbol: Symbol,
        start_ns: int,
        end_ns: int,
        limit: int,
        cursor: str | None = None,
    ) -> tuple[list[FundingRate], str | None]:
        """Fetch the full available funding history in one unpaginated response.

        ``GET /v4/historicalfundingrates`` takes only ``symbol`` and ignores
        any time/limit parameters: it always returns the entire ~1-year
        rolling window (~8.8k hourly entries), ascending. *limit* and *cursor*
        are therefore ignored and ``next_cursor`` is always ``None`` — one
        page IS the whole window. Entries carry ISO-8601 ``Z`` timestamps;
        ``rate`` stores ``relativeFundingRate`` (the comparable per-period
        rate), NOT the absolute per-contract ``fundingRate``. Entries outside
        ``[start_ns, end_ns]`` are filtered here (cheap; the paginator filters
        again — harmless).
        """
        params: dict[str, Any] = {"symbol": self.render_symbol(symbol)}
        async with self._http as client:
            data = await client.get(f"{_BASE}/v4/historicalfundingrates", params)

        if data.get("result") != "success":
            logger.error("Kraken Futures funding error: %s", data)
            return [], None

        rates = []
        for e in data.get("rates", []):
            dt = datetime.fromisoformat(e["timestamp"].replace("Z", "+00:00"))
            # ms precision is exact in float64 at this epoch; ns would not be.
            ts = int(dt.timestamp() * 1000) * 1_000_000
            if start_ns <= ts <= end_ns:
                rates.append(FundingRate(ts=ts, rate=float(e["relativeFundingRate"])))
        return rates, None

    async def fetch_ohlc_page(
        self,
        symbol: Symbol,
        span: int,
        start_ns: int,
        end_ns: int,
        limit: int,
    ) -> list[OHLCBar]:
        """Fetch one page of perp klines from the charts API.

        ``GET /api/charts/v1/trade/{symbol}/{resolution}`` with ``from``/``to``
        in **seconds** (live-probed; the response candle ``time`` is in ms).
        The response is ascending, anchored on ``from``, ~2 000 candles max
        per call — the generic forward paginator windows accordingly
        (``max_per_request=2000``). OHLCV fields arrive as **strings** and are
        parsed to floats; the endpoint provides no quote volume or trade
        count, so those stay ``None`` (never fabricated). Unsupported *span*
        → ``[]`` without any HTTP call.
        """
        resolution = kraken_futures_resolution(span)
        if not resolution:
            return []

        params: dict[str, Any] = {
            "from": start_ns // NS,
            "to": end_ns // NS,
        }
        url = f"{_BASE_CHARTS}/trade/{self.render_symbol(symbol)}/{resolution}"
        async with self._http as client:
            data = await client.get(url, params)

        return [
            OHLCBar(
                ts=int(c["time"]) * 1_000_000,
                open=float(c["open"]),
                high=float(c["high"]),
                low=float(c["low"]),
                close=float(c["close"]),
                volume=float(c["volume"]),
                quote_volume=None,
                trades=None,
            )
            for c in data.get("candles", [])
        ]
