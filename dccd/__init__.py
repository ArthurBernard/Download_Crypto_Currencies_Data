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

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version

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
        from dccd.application.service_factory import build_registry, build_store

        try:
            path = resolve_config_path(self._config_path)
            self._config = load_config(path)
        except FileNotFoundError:
            self._config = AppConfig()

        # Single source of truth for adapter wiring — same as CLI and API.
        self._store = build_store(self._config.settings.data_path)
        self._registry = build_registry()
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
            params=JobParams(start=start),
            origin="runtime",
        )
        return await do_backfill(spec, registry=self._registry, store=self._store)

    async def stream(self, exchange: str, symbol: str, data_type: str = "trades",
                     span: int | None = None, depth: int | None = None,
                     snapshot_interval: int | None = None,
                     stop_event: "object | None" = None) -> None:
        """Stream live data until *stop_event* is set.

        Parameters mirror :meth:`backfill`; ``stop_event`` is an
        :class:`asyncio.Event` used to stop the stream cleanly.
        """
        from dccd.application.jobs import JobParams, JobSpec, JobTarget, Trigger
        from dccd.application.operations import stream as do_stream
        from dccd.domain.symbol import Symbol
        from dccd.domain.types import DataType

        target = JobTarget(exchange=exchange, symbol=Symbol.parse(symbol),
                           data_type=DataType(data_type), span=span)
        spec = JobSpec(
            id=JobSpec.make_id("stream", target),
            operation="stream",
            target=target,
            trigger=Trigger(kind="supervised"),
            params=JobParams(depth=depth, snapshot_interval=snapshot_interval),
            origin="runtime",
        )
        await do_stream(spec, registry=self._registry, store=self._store,
                        stop_event=stop_event)  # type: ignore[arg-type]

    def read(self, exchange: str, symbol: str, data_type: str = "ohlc",
             span: int | None = None, start_ns: int | None = None,
             end_ns: int | None = None):
        """Read stored data for a dataset as a Polars DataFrame."""
        from dccd.application.jobs import JobTarget
        from dccd.application.operations import read as do_read
        from dccd.domain.symbol import Symbol
        from dccd.domain.types import DataType

        target = JobTarget(exchange=exchange, symbol=Symbol.parse(symbol),
                           data_type=DataType(data_type), span=span)
        return do_read(target, store=self._store, start_ns=start_ns, end_ns=end_ns)

    def inventory(self) -> list:
        """List stored datasets."""
        from dccd.application.operations import inventory
        return inventory(store=self._store)
