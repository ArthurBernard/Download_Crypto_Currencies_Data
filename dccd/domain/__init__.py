"""dccd domain layer — pure, sync, zero I/O."""

from dccd.domain.capability import Capability
from dccd.domain.dataset import DatasetId, Provenance
from dccd.domain.errors import CoverageError, NoCapability
from dccd.domain.records import OHLCBar, OrderBookLevel, OrderBookSnapshot, Trade
from dccd.domain.symbol import Symbol
from dccd.domain.types import DataType

__all__ = [
    "Capability",
    "CoverageError",
    "DataType",
    "DatasetId",
    "NoCapability",
    "OHLCBar",
    "OrderBookLevel",
    "OrderBookSnapshot",
    "Provenance",
    "Symbol",
    "Trade",
]
