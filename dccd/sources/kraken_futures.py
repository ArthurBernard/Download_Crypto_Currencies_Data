"""Kraken Futures source adapter — hourly perp funding history (~1-year rolling window)."""

from __future__ import annotations

# Built-in
import logging
from datetime import datetime
from typing import Any

# Local
from dccd.domain.capability import Capability
from dccd.domain.records import FundingRate
from dccd.domain.symbol import Symbol
from dccd.domain.types import DataType
from dccd.sources.base import FundingHistory, default_http_client
from dccd.transport.http import AsyncHTTPClient

__all__ = ["KrakenFuturesSource"]

logger = logging.getLogger(__name__)

_BASE = "https://futures.kraken.com/derivatives/api"

# Kraken names Bitcoin XBT on both spot and futures; canonical Symbols carry
# BTC (Symbol.parse normalises the other direction), so alias at render time.
_KRAKEN_ALIASES = {"BTC": "XBT"}


class KrakenFuturesSource(FundingHistory):
    """Kraken Futures source adapter (linear ``PF_`` perpetuals, funding only).

    Kraken Futures is a **separate API surface** from spot Kraken — different
    host (``futures.kraken.com``), different symbology (``PF_XBTUSD``) and
    different JSON shapes — so it gets its own adapter and exchange name
    (``krakenfutures``) rather than extra methods on
    :class:`~dccd.sources.kraken.KrakenSource`.

    - **Backfill**: realized funding rates (``perp`` market only), at Kraken's
      **1-hour cadence** — denser than the ~8h cadence of Binance/Bybit;
      normalise before any cross-exchange comparison. The endpoint serves a
      hard **~1-year rolling window** in a single unpaginated response
      (``history="recent"`` + ``recent_window_s``); run a recurring job to
      accumulate history forward.

    No WebSocket channels, OHLC or open interest are declared — Kraken
    Futures has no OI *history* endpoint (snapshot only), and undeclared
    capabilities must stay undeclared (honesty invariant).

    See Also
    --------
    dccd.Client : the public facade.
    dccd.sources.kraken.KrakenSource : the spot adapter (separate API).

    Examples
    --------
    >>> from dccd.sources.kraken_futures import KrakenFuturesSource
    >>> [c.data_type.value for c in KrakenFuturesSource().capabilities()]
    ['funding']
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
