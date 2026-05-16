#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2019-08-30 09:28:43
# @Last modified by: ArthurBernard
# @Last modified time: 2019-08-30 10:01:33

""" Basis object to download continuously data from websocket. """

# Built-in packages
import asyncio
import json
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, AsyncIterator

# Third party packages
# Local packages
from dccd.models import Trade
from dccd.process_data import set_marketdepth, set_trades
from dccd.tools.websocket import BasisWebSocket

__all__ = ['ContinuousDownloader']


class ContinuousDownloader(BasisWebSocket):
    """ Basis object to download data from a stream websocket client API.

    Parameters
    ----------
    host : str
        WebSocket URL of the exchange, or one of the magic strings
        ``'bitfinex'`` / ``'bitmex'`` for pre-configured connections.
        For all other exchanges pass the full URL directly.
    time_step : int or None, optional
        Number of seconds between two snapshots of data, minimum is 1,
        default is 60 (one minute). Each ``time_step`` seconds data will be
        processed and pushed to the database.  Pass ``None`` to receive data
        tick-by-tick without periodic aggregation.
    STOP : int, optional
        Number of seconds before stoping, default is `3600` (one hour).
    kwargs : dict, optional
        Connection and subscribe parameters, relevant only if host is not
        allowed in `_parser_exchange`.

    Attributes
    ----------
    host : str
        Adress of host to connect.
    conn_par : dict
        Parameters of websocket connection.
    ws : websockets.client.WebSocketClientProtocol
        Connection with the websocket client.
    is_connect : bool
        True if is connected, False otherwise.
    ts : int
        Number of second between two snapshots of data.
    t : int
        Current timestamp but rounded by `ts`.
    until : int
        Timestamp to stop to download data.

    Methods
    -------
    set_process_data
    set_saver

    """

    _parser_exchange: dict[str, Any] = {
        'bitfinex': {
            'host': 'wss://api-pub.bitfinex.com/ws/2',
            'subs': {'event': 'subscribe'},  # , 'stream': None},
            'conn': {
                'ping_interval': 5,
                'ping_timeout': 5,
                'close_timeout': 5,
            }
        },
        'bitmex': {
            'host': 'wss://www.bitmex.com/realtime',
            'subs': {'op': 'subscribe'},  # , 'args': None},
        },
    }
    _parser_data: dict[str, Callable[..., Any]] = {}

    def __init__(self, host: str, time_step: int = 60, STOP: int = 3600,
                 checkpoint_dir: str | None = None, **kwargs: Any) -> None:
        """ Initialize object. """
        if host.lower() in ContinuousDownloader._parser_exchange.keys():
            BasisWebSocket.__init__(self, **self._parser_exchange[host])

        else:
            BasisWebSocket.__init__(self, host, **kwargs)

        # Set variables
        self.ts = max(time_step, 1)
        self.t = self._current_timestep()
        self.until = time.time() + STOP if STOP > 0 else time.time() * 10

        # Set data
        self._data: dict[int, dict[str, Any]] = {}
        self._checkpoint_dir: Path | None = Path(checkpoint_dir) if checkpoint_dir else None
        self.d: dict = {}

    def __aiter__(self) -> AsyncIterator[dict[str, Any] | None]:
        """ Set iterative method. """
        self.logger.debug('Starting generator websocket')

        return self

    async def __anext__(self) -> dict[str, Any] | None:
        """ Iterate object. """
        if time.time() > self.until:
            self.logger.info('StopIteration')
            self.on_close()

            raise StopAsyncIteration

        # Time to sleep
        time_wait = self.t + self.ts - time.time()

        if time_wait > 0:
            await asyncio.sleep(time_wait)

        t, self.t = self.t, self._current_timestep()

        if t in self._data:
            payload = self._data.pop(t)
            payload['snapshot_ts'] = int(time.time() * 1000)
            return payload

        return None

    async def _loop(self) -> None:
        """ Loop to process and save data into database. """
        await self.wait_that('is_connect')

        async for snapshot in self:
            if snapshot is None:
                self.logger.debug('No data')
                continue

            trades = snapshot.get('trades', [])
            book = snapshot.get('book', {})
            ts = snapshot['snapshot_ts']

            if trades and hasattr(self, '_trades_saver'):
                df = self._trades_process_func(trades)
                self._trades_saver(df, **self._trades_saver_kwargs)

            if book and hasattr(self, '_book_saver'):
                df = self._book_process_func(book, t=ts // 1000)
                self._book_saver(df, **self._book_saver_kwargs)

            self._save_checkpoint()

            # Legacy fallback for callers that still use set_process_data + set_saver
            if not (hasattr(self, '_trades_saver') or hasattr(self, '_book_saver')):
                if hasattr(self, 'process_data') and hasattr(self, 'saver'):
                    legacy_data = trades if trades else book
                    if legacy_data:
                        df = self.process_data(legacy_data, **self.process_params)
                        self.saver(df, **self.io_params)

            self.logger.debug(
                'snapshot_ts=%d trades=%d book_levels=%d', ts, len(trades), len(book)
            )

            if not self.is_connect:
                return

    async def on_message(self, data: dict[str, Any] | list[Any]) -> None:
        """ Parse any data received from the websocket. """
        self._raw_parser(data)

    def _raw_parser(self, data: Any) -> None:
        self._data.setdefault(self.t, {'trades': [], 'book': {}})['trades'].append(data)

    def _current_timestep(self) -> int:
        """ Set current time rounded by `timestep`. """
        return int((time.time() + 0.001) // self.ts * self.ts)

    def __call__(self, *args: Any, **kwargs: Any) -> 'ContinuousDownloader':
        """ Start the WebSocket stream and block until it stops.

        Parameters
        ----------
        *args, **kwargs
            Forwarded to :meth:`~dccd.tools.websocket.BasisWebSocket._connect`.

        Returns
        -------
        ContinuousDownloader
            The downloader instance (for chaining).

        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(asyncio.gather(
                self._connect(*args, **kwargs),
                self._loop(),
            ))
        finally:
            loop.close()
        return self

    def _push_trades(self, parsed: list[dict[str, Any]]) -> None:
        """ Validate and store a normalised list of trade dicts. """
        for item in parsed:
            Trade.model_validate(item)
            self._raw_parser(item)

    def _push_book_updates(self, updates: dict[str, Any]) -> None:
        """ Apply a price→qty update dict to the local book and snapshot it. """
        for price, qty in updates.items():
            if qty == 0:
                self.d.pop(price, None)
            else:
                self.d[price] = qty
        self._data.setdefault(self.t, {'trades': [], 'book': {}})['book'] = dict(self.d)

    def _get_book_state(self) -> dict:
        return dict(self.d)

    def _restore_book_state(self, state: dict) -> None:
        self.d = state

    def _checkpoint_file(self) -> Path | None:
        if self._checkpoint_dir is None:
            return None
        name = getattr(self, 'pair', 'default')
        return self._checkpoint_dir / f'{name}_book.json'

    def _save_checkpoint(self) -> None:
        f = self._checkpoint_file()
        if f is None:
            return
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_text(json.dumps(self._get_book_state()))

    def _load_checkpoint(self) -> None:
        f = self._checkpoint_file()
        if f and f.exists():
            self._restore_book_state(json.loads(f.read_text()))

    def set_trades_saver(self, saver: Callable[..., Any],
                         process_func: Callable[..., Any] = set_trades,
                         **kwargs: Any) -> None:
        """ Set saver for the trades channel.

        Parameters
        ----------
        saver : callable
            Callable to persist the processed DataFrame (e.g. ``IODataBase``).
        process_func : callable, optional
            Function to convert raw trade list to a DataFrame, default is
            :func:`dccd.process_data.set_trades`.
        **kwargs
            Extra keyword arguments forwarded to ``saver`` on each call.

        """
        self._trades_saver = saver
        self._trades_process_func = process_func
        self._trades_saver_kwargs = kwargs

    def set_book_saver(self, saver: Callable[..., Any],
                       process_func: Callable[..., Any] = set_marketdepth,
                       **kwargs: Any) -> None:
        """ Set saver for the order-book channel.

        Parameters
        ----------
        saver : callable
            Callable to persist the processed DataFrame (e.g. ``IODataBase``).
        process_func : callable, optional
            Function to convert the book dict to a DataFrame, default is
            :func:`dccd.process_data.set_marketdepth`.
        **kwargs
            Extra keyword arguments forwarded to ``saver`` on each call.

        """
        self._book_saver = saver
        self._book_process_func = process_func
        self._book_saver_kwargs = kwargs

    def set_process_data(self, func: Callable[..., Any], **kwargs: Any) -> None:
        """ Set processing function.

        Parameters
        ----------
        func : callable
            Function to process and clean data before to be saved. Must take
            `data` in arguments and can take any optional keywords arguments,
            cf exemples in :mod:`dccd.process_data`.
        **kwargs
            Any keyword arguments to be passed to `func`.

        """
        self.process_data = func
        self.process_params = kwargs

    def set_saver(self, call: Callable[..., Any], **kwargs: Any) -> None:
        """ Set saver object to save data or update a database.

        Parameters
        ----------
        call : callable
            Callable object to save data or update a database. Must take `data`
            in arguments and can take any optional keywords arguments, cf
            exemples in :mod:`dccd.io_tools`.
        **kwargs
            Any keyword arguments to be passed to `call`.

        """
        self.saver = call
        self.io_params = kwargs

    def get_parser(self, key: str) -> Callable[..., Any]:
        """ Get allowed data parser.

        Parameters
        ----------
        key : str
            Name code of data to parse.

        Returns
        -------
        function
            The allowed function to parse this kind of data.

        Raises
        ------
        KeyError
            If `key` is not in the allowed parser keys.

        """
        if key not in self._parser_data:
            raise KeyError(
                f"Unknown parser key {key!r}, allowed: {list(self._parser_data)}"
            )

        return self._parser_data[key]
