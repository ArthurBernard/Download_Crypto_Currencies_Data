"""Source registry and resolver."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from dccd.domain.errors import NoCapability
from dccd.domain.types import DataType
from dccd.sources.base import (
    OHLCHistory,
    OHLCLive,
    OrderBookLive,
    OrderBookSnapshotREST,
    Source,
    TradesHistory,
    TradesLive,
)

if TYPE_CHECKING:
    pass

__all__ = ["SourceRegistry"]

logger = logging.getLogger(__name__)


class SourceRegistry:
    """Maps exchange names to source adapter instances.

    Examples
    --------
    >>> reg = SourceRegistry()
    >>> # reg.register('binance', BinanceSource())
    >>> # src = reg.get_ohlc_history('binance')
    """

    def __init__(self) -> None:
        self._adapters: dict[str, Source] = {}

    def register(self, exchange: str, adapter: Source) -> None:
        """Register an adapter for an exchange."""
        self._adapters[exchange.lower()] = adapter

    def get(self, exchange: str) -> Source:
        """Return the adapter registered for *name* (raises NoCapability if absent)."""
        exchange = exchange.lower()
        if exchange not in self._adapters:
            raise NoCapability(exchange, "*", "*", "no adapter registered")
        return self._adapters[exchange]

    def get_ohlc_history(self, exchange: str) -> OHLCHistory:
        """Return *name* as an :class:`~dccd.sources.base.OHLCHistory` or raise NoCapability."""
        adapter = self.get(exchange)
        if not isinstance(adapter, OHLCHistory):
            raise NoCapability(exchange, "ohlc", "historical", "adapter does not implement OHLCHistory")
        return adapter

    def get_trades_history(self, exchange: str) -> TradesHistory:
        """Return *name* as a :class:`~dccd.sources.base.TradesHistory` or raise NoCapability."""
        adapter = self.get(exchange)
        if not isinstance(adapter, TradesHistory):
            raise NoCapability(exchange, "trades", "historical", "adapter does not implement TradesHistory")
        return adapter

    def get_orderbook_snapshot(self, exchange: str) -> OrderBookSnapshotREST:
        """Return *name* as an :class:`~dccd.sources.base.OrderBookSnapshotREST` or raise NoCapability."""
        adapter = self.get(exchange)
        if not isinstance(adapter, OrderBookSnapshotREST):
            raise NoCapability(exchange, "orderbook", "snapshot", "adapter does not implement OrderBookSnapshotREST")
        return adapter

    def get_ohlc_live(self, exchange: str) -> OHLCLive:
        """Return *name* as an :class:`~dccd.sources.base.OHLCLive` or raise NoCapability."""
        adapter = self.get(exchange)
        if not isinstance(adapter, OHLCLive):
            raise NoCapability(exchange, "ohlc", "live", "adapter does not implement OHLCLive")
        return adapter

    def get_trades_live(self, exchange: str) -> TradesLive:
        """Return *name* as a :class:`~dccd.sources.base.TradesLive` or raise NoCapability."""
        adapter = self.get(exchange)
        if not isinstance(adapter, TradesLive):
            raise NoCapability(exchange, "trades", "live", "adapter does not implement TradesLive")
        return adapter

    def get_orderbook_live(self, exchange: str) -> OrderBookLive:
        """Return *name* as an :class:`~dccd.sources.base.OrderBookLive` or raise NoCapability."""
        adapter = self.get(exchange)
        if not isinstance(adapter, OrderBookLive):
            raise NoCapability(exchange, "orderbook", "live", "adapter does not implement OrderBookLive")
        return adapter

    def resolve(
        self,
        exchange: str,
        data_type: DataType,
        transport: str,
        mode: str,
    ) -> Source:
        """Return appropriate adapter or raise NoCapability."""
        adapter = self.get(exchange)
        cap = adapter.capability_for(data_type, transport, mode)
        if cap is None:
            raise NoCapability(exchange, data_type.value, f"{transport}/{mode}")
        return adapter

    @property
    def exchanges(self) -> list[str]:
        """Names of all registered exchanges."""
        return list(self._adapters.keys())
