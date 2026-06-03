"""Service-object factory.

Central place that wires together all exchange adapters so that both the CLI
and the API import from one location. Adding a new exchange means editing only
this file.
"""

from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dccd.sources.registry import SourceRegistry
    from dccd.storage.parquet import ParquetStore
    from dccd.storage.runs_sqlite import RunsStore

__all__ = ["build_registry", "build_store", "build_runs_store"]


def build_registry() -> "SourceRegistry":
    """Return a :class:`~dccd.sources.registry.SourceRegistry` with all adapters registered.

    Returns
    -------
    SourceRegistry
    """
    from dccd.sources.binance import BinanceSource
    from dccd.sources.bitfinex import BitfinexSource
    from dccd.sources.bitmex import BitMEXSource
    from dccd.sources.bybit import BybitSource
    from dccd.sources.coinbase import CoinbaseSource
    from dccd.sources.kraken import KrakenSource
    from dccd.sources.okx import OKXSource
    from dccd.sources.registry import SourceRegistry

    reg = SourceRegistry()
    reg.register("binance", BinanceSource())
    reg.register("coinbase", CoinbaseSource())
    reg.register("kraken", KrakenSource())
    reg.register("bybit", BybitSource())
    reg.register("okx", OKXSource())
    reg.register("bitfinex", BitfinexSource())
    reg.register("bitmex", BitMEXSource())
    return reg


def build_store(data_path: str | pathlib.Path) -> "ParquetStore":
    """Return a :class:`~dccd.storage.parquet.ParquetStore` for *data_path*.

    Parameters
    ----------
    data_path : str or Path

    Returns
    -------
    ParquetStore
    """
    from dccd.storage.parquet import ParquetStore

    return ParquetStore(data_path)


def build_runs_store(data_path: str | pathlib.Path) -> "RunsStore":
    """Return a :class:`~dccd.storage.runs_sqlite.RunsStore` inside *data_path*.

    The database lives at ``{data_path}/.dccd/runs.db``.

    Parameters
    ----------
    data_path : str or Path

    Returns
    -------
    RunsStore
    """
    from dccd.storage.runs_sqlite import RunsStore

    return RunsStore(pathlib.Path(data_path) / ".dccd" / "runs.db")
