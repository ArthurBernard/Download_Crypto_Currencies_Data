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

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import asyncio

    import polars as pl

    from dccd.application.config import AppConfig
    from dccd.sources.registry import SourceRegistry
    from dccd.storage.parquet import ParquetStore

__all__ = ["__version__", "Client"]


class Client:
    """Async facade for dccd — the one-stop entry point.

    Wires every exchange adapter and the local Parquet store, and exposes the
    four operations as methods: :meth:`backfill` (download history),
    :meth:`stream` (collect live), :meth:`read` (load stored data) and
    :meth:`inventory` (list datasets). Use it as an async context manager so the
    shared HTTP client is opened and closed cleanly.

    Parameters
    ----------
    config_path : str or None
        Path to ``config.yml``. Resolved via the XDG fallback when ``None``;
        only ``settings.data_path`` is needed for direct use.

    See Also
    --------
    dccd.application.operations.backfill : the underlying operation.

    Examples
    --------
    >>> import asyncio
    >>> async def main():
    ...     async with Client() as c:
    ...         await c.backfill('binance', 'BTC/USDT', 'ohlc', span=3600,
    ...                          start='2024-01-01')
    ...         return c.read('binance', 'BTC/USDT', 'ohlc', span=3600).height
    >>> asyncio.run(main())  # doctest: +SKIP
    168
    """

    def __init__(self, config_path: str | None = None) -> None:
        self._config_path = config_path
        self._config: AppConfig | None = None
        self._store: ParquetStore | None = None
        self._registry: SourceRegistry | None = None

    def _require_ready(self) -> tuple["SourceRegistry", "ParquetStore"]:
        if self._registry is None or self._store is None:
            raise RuntimeError("Client must be used inside 'async with Client() as c:'")
        return self._registry, self._store

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

    async def __aexit__(self, *args: Any) -> None:
        pass

    async def backfill(self, exchange: str, symbol: str, data_type: str = "ohlc",
                       span: int | None = None, start: str = "last") -> dict[str, Any]:
        """Download historical data for one dataset into the local store.

        Resumes and deduplicates: running it again only adds what is missing.
        Trades are cursor-paginated and drain the whole requested window.

        Parameters
        ----------
        exchange : str
            Exchange name, e.g. ``'binance'``. See :doc:`/exchanges`.
        symbol : str
            Trading pair, ``'BTC/USDT'`` or ``'BTC-USD'``.
        data_type : str, default 'ohlc'
            ``'ohlc'``, ``'trades'`` or ``'orderbook'``.
        span : int or None
            Candle size in seconds — **required** for ``'ohlc'``.
        start : str, default 'last'
            ``'last'`` (resume from the last stored row), ``'origin'`` (full
            history) or an ISO date such as ``'2024-01-01'``.

        Returns
        -------
        dict
            ``{'run_id', 'rows_written', 'start_ns', 'end_ns'}`` on success;
            ``{'run_id', 'rows_written', 'error'}`` on failure.

        See Also
        --------
        read : load the result. stream : live collection instead of history.

        Examples
        --------
        >>> async def main():
        ...     async with Client() as c:
        ...         r = await c.backfill('binance', 'BTC/USDT', 'ohlc',
        ...                              span=3600, start='2024-01-01')
        ...         return r['rows_written']
        >>> asyncio.run(main())  # doctest: +SKIP
        168
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
        registry, store = self._require_ready()
        return await do_backfill(spec, registry=registry, store=store)

    async def stream(self, exchange: str, symbol: str, data_type: str = "trades",
                     span: int | None = None, depth: int | None = None,
                     snapshot_interval: int | None = None,
                     stop_event: "asyncio.Event | None" = None) -> None:
        """Collect live data over WebSocket until *stop_event* is set.

        Parameters mirror :meth:`backfill`. ``depth`` / ``snapshot_interval``
        apply to order-book streams. For long-running collection prefer a
        ``supervised`` stream job in the config and ``dccd start`` (auto-reconnect).

        Parameters
        ----------
        exchange, symbol : str
        data_type : str, default 'trades'
            ``'ohlc'``, ``'trades'`` or ``'orderbook'``.
        span, depth, snapshot_interval : int or None
        stop_event : asyncio.Event or None
            Set it to stop the stream cleanly.

        See Also
        --------
        backfill : download history instead of live data.

        Examples
        --------
        >>> async def main():
        ...     async with Client() as c:
        ...         stop = asyncio.Event()
        ...         asyncio.get_running_loop().call_later(10, stop.set)
        ...         await c.stream('binance', 'BTC/USDT', 'trades', stop_event=stop)
        >>> asyncio.run(main())  # doctest: +SKIP
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
        registry, store = self._require_ready()
        await do_stream(spec, registry=registry, store=store, stop_event=stop_event)

    def read(self, exchange: str, symbol: str, data_type: str = "ohlc",
             span: int | None = None, start_ns: int | None = None,
             end_ns: int | None = None) -> "pl.DataFrame":
        """Read stored data for a dataset as a Polars DataFrame.

        Parameters
        ----------
        exchange, symbol : str
        data_type : str, default 'ohlc'
        span : int or None
            Required for ``'ohlc'``.
        start_ns, end_ns : int or None
            Optional inclusive nanosecond bounds.

        Returns
        -------
        polars.DataFrame
            Sorted by ``TS`` (nanoseconds UTC), deduplicated. Empty if no data.

        See Also
        --------
        backfill : populate the dataset. inventory : list what is stored.

        Examples
        --------
        >>> async def main():
        ...     async with Client() as c:
        ...         return c.read('binance', 'BTC/USDT', 'ohlc', span=3600).columns
        >>> asyncio.run(main())  # doctest: +SKIP
        ['TS', 'open', 'high', 'low', 'close', 'volume', 'quote_volume', 'trades']
        """
        from typing import cast

        from dccd.application.jobs import JobTarget
        from dccd.application.operations import read as do_read
        from dccd.domain.symbol import Symbol
        from dccd.domain.types import DataType

        _, store = self._require_ready()
        target = JobTarget(exchange=exchange, symbol=Symbol.parse(symbol),
                           data_type=DataType(data_type), span=span)
        return cast("pl.DataFrame", do_read(target, store=store, start_ns=start_ns, end_ns=end_ns))

    def inventory(self) -> list[dict[str, Any]]:
        """List every stored dataset with its coverage.

        Returns
        -------
        list of dict
            One entry per dataset with ``exchange``, ``pair``, ``data_type``,
            ``span``, ``rows``, ``min_ts`` and ``max_ts`` (nanoseconds UTC).

        Examples
        --------
        >>> async def main():
        ...     async with Client() as c:
        ...         return [d['pair'] for d in c.inventory()]
        >>> asyncio.run(main())  # doctest: +SKIP
        ['BTC-USDT']
        """
        from dccd.application.operations import inventory
        _, store = self._require_ready()
        return inventory(store=store)
