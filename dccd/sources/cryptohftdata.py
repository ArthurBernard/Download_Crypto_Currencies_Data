"""CryptoHFTData multi-venue historical trade adapter."""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from dccd.domain.capability import Capability
from dccd.domain.records import Trade
from dccd.domain.symbol import Symbol
from dccd.domain.types import DataType
from dccd.sources.base import TradesHistory

__all__ = [
    "CRYPTOHFTDATA_VENUES",
    "CryptoHFTDataSource",
    "build_cryptohftdata_sources",
]

_NS_PER_DAY = 86_400_000_000_000
_PAGE_STATE_TTL_S = 3_600
_MAX_PAGE_STATES = 32

# Public dccd source name -> (CryptoHFTData venue, supported non-spot markets).
# ``None`` retains Capability's spot-only default.
CRYPTOHFTDATA_VENUES: dict[str, tuple[str, list[str] | None]] = {
    "cryptohftdata-binance-spot": ("binance_spot", None),
    "cryptohftdata-binance-futures": ("binance_futures", ["perp"]),
    "cryptohftdata-bybit-spot": ("bybit_spot", None),
    "cryptohftdata-bybit-futures": ("bybit", ["perp"]),
    "cryptohftdata-kraken-spot": ("kraken_spot", None),
    "cryptohftdata-kraken-derivatives": ("kraken_derivatives", ["perp"]),
    "cryptohftdata-okx-spot": ("okx_spot", None),
    "cryptohftdata-okx-futures": ("okx_futures", ["perp"]),
    "cryptohftdata-bitget-spot": ("bitget_spot", None),
    "cryptohftdata-bitget-futures": ("bitget_futures", ["perp"]),
    "cryptohftdata-hyperliquid-spot": ("hyperliquid_spot", None),
    "cryptohftdata-hyperliquid-futures": ("hyperliquid_futures", ["perp"]),
    "cryptohftdata-lighter": ("lighter", ["perp"]),
    "cryptohftdata-aster-futures": ("aster_futures", ["perp"]),
    "cryptohftdata-bitmex": ("bitmex", ["perp"]),
}


class _Client(Protocol):
    """Subset of the CryptoHFTData SDK used by this adapter."""

    def get_trades(self, **kwargs: Any) -> Any:
        """Return a pandas DataFrame of exchange-native trades."""


@dataclass
class _PageState:
    """Cursor state for bounded in-memory paging of one-day SDK responses."""

    symbol: str
    start_ns: int
    end_ns: int
    next_day_ns: int
    rows: list[Trade]
    offset: int = 0
    generation: int = 0
    touched_at: float = 0.0


class CryptoHFTDataSource(TradesHistory):
    """Serve one CryptoHFTData venue through dccd's historical trade protocol.

    The SDK downloads date-partitioned Parquet archives synchronously. Calls run
    in a worker thread so dccd's event loop remains responsive. Each downloaded
    day is converted to canonical nanosecond ``Trade`` records and exposed in
    bounded cursor pages; completed and stale cursor state is discarded.
    """

    def __init__(
        self,
        exchange: str,
        venue: str,
        markets: list[str] | None = None,
        *,
        api_key: str | None = None,
        client: _Client | None = None,
    ) -> None:
        self.exchange = exchange
        self.venue = venue
        self._markets = markets
        self._api_key = api_key
        self._client = client
        self._download_lock = asyncio.Lock()
        self._states: dict[str, _PageState] = {}

    def capabilities(self) -> list[Capability]:
        """Declare full historical trade coverage for this provider venue."""
        return [
            Capability(
                data_type=DataType.TRADES,
                transport="rest",
                mode="historical",
                history="full",
                max_per_request=10_000,
                page_direction="forward",
                markets=self._markets,
            )
        ]

    def render_symbol(self, symbol: Symbol) -> str:
        """Render a canonical pair as the archive's compact uppercase symbol."""
        return f"{symbol.base}{symbol.quote}".upper()

    async def fetch_trades_page(
        self,
        symbol: Symbol,
        start_ns: int,
        end_ns: int,
        limit: int,
        cursor: str | None = None,
    ) -> tuple[list[Trade], str | None]:
        """Fetch a bounded page from the inclusive ``[start_ns, end_ns]`` range."""
        if limit < 1:
            raise ValueError("CryptoHFTData page limit must be positive")
        if end_ns < start_ns:
            raise ValueError("CryptoHFTData end_ns must not precede start_ns")

        rendered = self.render_symbol(symbol)
        if cursor is None:
            self._prune_states()
            state = _PageState(
                symbol=rendered,
                start_ns=start_ns,
                end_ns=end_ns,
                next_day_ns=(start_ns // _NS_PER_DAY) * _NS_PER_DAY,
                rows=[],
                touched_at=time.monotonic(),
            )
            token = uuid.uuid4().hex
        else:
            token = cursor.partition(":")[0]
            existing_state = self._states.get(token)
            if existing_state is None:
                raise ValueError("CryptoHFTData cursor is expired or invalid")
            state = existing_state
            if (state.symbol, state.start_ns, state.end_ns) != (rendered, start_ns, end_ns):
                raise ValueError("CryptoHFTData cursor does not match this request")

        while state.offset >= len(state.rows) and state.next_day_ns <= end_ns:
            day_start_ns = state.next_day_ns
            state.next_day_ns += _NS_PER_DAY
            state.rows = await self._download_day(rendered, day_start_ns, start_ns, end_ns)
            state.offset = 0

        page = state.rows[state.offset : state.offset + limit]
        state.offset += len(page)
        has_more = state.offset < len(state.rows) or state.next_day_ns <= end_ns
        if not has_more:
            self._states.pop(token, None)
            return page, None

        state.generation += 1
        state.touched_at = time.monotonic()
        self._states[token] = state
        return page, f"{token}:{state.generation}"

    async def _download_day(
        self,
        symbol: str,
        day_start_ns: int,
        request_start_ns: int,
        request_end_ns: int,
    ) -> list[Trade]:
        day = datetime.fromtimestamp(day_start_ns / 1_000_000_000, tz=timezone.utc).date()
        client = self._get_client()
        async with self._download_lock:
            frame = await asyncio.to_thread(
                client.get_trades,
                symbol=symbol,
                exchange=self.venue,
                start_date=day.isoformat(),
                end_date=day.isoformat(),
            )
        if frame.empty:
            return []

        required = {"trade_time", "trade_id", "price", "quantity", "is_buyer_maker"}
        missing = required.difference(frame.columns)
        if missing:
            raise ValueError(
                "CryptoHFTData response is missing required columns: "
                + ", ".join(sorted(missing))
            )

        rows: list[Trade] = []
        ordered = frame.sort_values(["trade_time", "trade_id"], kind="stable")
        day_end_ns = day_start_ns + _NS_PER_DAY
        for row in ordered.itertuples(index=False):
            timestamp_ns = int(row.trade_time) * 1_000_000
            if (
                day_start_ns <= timestamp_ns < day_end_ns
                and request_start_ns <= timestamp_ns <= request_end_ns
            ):
                rows.append(
                    Trade(
                        ts=timestamp_ns,
                        price=float(row.price),
                        amount=float(row.quantity),
                        side="sell" if bool(row.is_buyer_maker) else "buy",
                        tid=str(row.trade_id),
                    )
                )
        return rows

    def _get_client(self) -> _Client:
        if self._client is None:
            try:
                from cryptohftdata import CryptoHFTDataClient
            except ImportError as exc:
                raise ImportError(
                    "CryptoHFTData sources require the optional dependency; "
                    "install dccd[cryptohftdata]"
                ) from exc
            api_key = self._api_key or os.getenv("CRYPTOHFTDATA_API_KEY") or None
            self._client = CryptoHFTDataClient(api_key=api_key)
        return self._client

    def _prune_states(self) -> None:
        now = time.monotonic()
        stale = [
            token
            for token, state in self._states.items()
            if now - state.touched_at > _PAGE_STATE_TTL_S
        ]
        for token in stale:
            self._states.pop(token, None)
        if len(self._states) >= _MAX_PAGE_STATES:
            oldest = min(self._states, key=lambda token: self._states[token].touched_at)
            self._states.pop(oldest, None)


def build_cryptohftdata_sources(
    *, api_key: str | None = None
) -> dict[str, CryptoHFTDataSource]:
    """Build all provider-qualified CryptoHFTData venue adapters."""
    return {
        exchange: CryptoHFTDataSource(
            exchange=exchange,
            venue=venue,
            markets=markets,
            api_key=api_key,
        )
        for exchange, (venue, markets) in CRYPTOHFTDATA_VENUES.items()
    }
