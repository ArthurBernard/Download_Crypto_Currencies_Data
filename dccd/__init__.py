"""dccd — Download Crypto Currency Data v3.

Three usage modes:
  1. Python API   — ``from dccd import Client``
  2. CLI daemon   — ``dccd start --config config.yml``
  3. HTTP API/UI  — ``dccd ui --config config.yml``

Examples
--------
>>> from dccd import __version__
>>> isinstance(__version__, str)
True
"""

from importlib.metadata import PackageNotFoundError, version as _pkg_version

try:
    __version__: str = _pkg_version("dccd")
except PackageNotFoundError:
    __version__ = "unknown"

__all__ = ["__version__", "Client"]


class Client:
    """Async context manager facade for dccd v3.

    Parameters
    ----------
    config_path : str or None
        Path to config.yml.  Resolved via XDG fallback when None.

    Examples
    --------
    >>> import asyncio
    >>> async def example():
    ...     from dccd import Client
    ...     async with Client() as c:
    ...         pass
    """

    def __init__(self, config_path: str | None = None) -> None:
        self._config_path = config_path
        self._config = None
        self._store = None
        self._registry = None

    async def __aenter__(self) -> "Client":
        from dccd.application.config import AppConfig, load_config, resolve_config_path
        from dccd.sources.binance import BinanceSource
        from dccd.sources.coinbase import CoinbaseSource
        from dccd.sources.kraken import KrakenSource
        from dccd.sources.bybit import BybitSource
        from dccd.sources.okx import OKXSource
        from dccd.sources.bitfinex import BitfinexSource
        from dccd.sources.bitmex import BitMEXSource
        from dccd.sources.registry import SourceRegistry
        from dccd.storage.parquet import ParquetStore

        try:
            path = resolve_config_path(self._config_path)
            self._config = load_config(path)
        except FileNotFoundError:
            self._config = AppConfig()

        self._store = ParquetStore(self._config.settings.data_path)
        self._registry = SourceRegistry()
        self._registry.register("binance", BinanceSource())
        self._registry.register("coinbase", CoinbaseSource())
        self._registry.register("kraken", KrakenSource())
        self._registry.register("bybit", BybitSource())
        self._registry.register("okx", OKXSource())
        self._registry.register("bitfinex", BitfinexSource())
        self._registry.register("bitmex", BitMEXSource())
        return self

    async def __aexit__(self, *args) -> None:
        pass

    async def backfill(self, exchange: str, symbol: str, data_type: str = "ohlc",
                       span: int | None = None, start: str = "last") -> dict:
        """Backfill one dataset.

        Parameters
        ----------
        exchange : str
        symbol : str
            E.g. ``'BTC/USDT'`` or ``'BTC-USD'``.
        data_type : str
            ``'ohlc'``, ``'trades'``, or ``'orderbook'``.
        span : int or None
            Required for OHLC.
        start : str
            ``'last'``, ``'origin'``, or ISO date.
        """
        from dccd.application.jobs import JobParams, JobSpec, JobTarget, Trigger
        from dccd.application.operations import backfill as do_backfill
        from dccd.domain.symbol import Symbol
        from dccd.domain.types import DataType

        sym = Symbol.parse(symbol)
        target = JobTarget(
            exchange=exchange,
            symbol=sym,
            data_type=DataType(data_type),
            span=span,
        )
        spec = JobSpec(
            id=JobSpec.make_id("backfill", target),
            operation="backfill",
            target=target,
            trigger=Trigger(kind="once"),
            params=JobParams(start=start),  # type: ignore[arg-type]
            origin="runtime",
        )
        return await do_backfill(spec, registry=self._registry, store=self._store)

    def inventory(self) -> list:
        """List stored datasets."""
        from dccd.application.operations import inventory
        return inventory(store=self._store)
