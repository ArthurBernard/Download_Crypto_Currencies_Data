"""DatasetId and Provenance — dataset identity and lineage."""

from __future__ import annotations

from pydantic import BaseModel

from dccd.domain.symbol import Symbol
from dccd.domain.types import DataType

__all__ = ["DatasetId", "Provenance"]


class DatasetId(BaseModel, frozen=True):
    """Unique identifier for a dataset (exchange × symbol × type × span).

    Examples
    --------
    >>> from dccd.domain.symbol import Symbol
    >>> from dccd.domain.types import DataType
    >>> ds = DatasetId(exchange='binance', symbol=Symbol(base='BTC', quote='USDT'), data_type=DataType.OHLC, span=3600)
    >>> ds.exchange
    'binance'
    """

    exchange: str
    symbol: Symbol
    data_type: DataType
    span: int | None = None

    def pair_slug(self) -> str:
        """Return filesystem-safe pair string (e.g. ``'BTC-USDT'``)."""
        return f"{self.symbol.base}-{self.symbol.quote}"

    def __str__(self) -> str:
        parts = [self.exchange, str(self.symbol), self.data_type.value]
        if self.span is not None:
            parts.append(f"{self.span}s")
        return ":".join(parts)


class Provenance(BaseModel, frozen=True):
    """Dataset-level lineage metadata (stored in Parquet metadata, not per-row).

    Attributes
    ----------
    source : str
        Origin description, e.g. ``"binance:rest"`` or ``"kraken:ws"``.
    derived_from : DatasetId or None
        Parent dataset when this dataset was derived (e.g. OHLC from trades).
    """

    source: str
    derived_from: DatasetId | None = None
