#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2019-08-07 11:16:51
# @Last modified by: ArthurBernard
# @Last modified time: 2019-09-11 08:46:12

""" Objects and functions to download data from Bitmex exchange.

.. currentmodule:: dccd.continuous_dl.bitmex

These functions and objects allow you to continuously download data and update
your database.

High level API
--------------

.. autofunction:: get_data_bitmex
.. autofunction:: get_orderbook_bitmex
.. autofunction:: get_trades_bitmex

Low level API
-------------

.. autoclass:: dccd.continuous_dl.bitmex.DownloadBitmexData
   :members: set_process_data, set_saver
   :special-members: __call__
   :show-inheritance:

"""

# Built-in packages
import asyncio
import time
from datetime import datetime as dt
from typing import Any

from dccd.continuous_dl.exchange import ContinuousDownloader
from dccd.process_data import set_marketdepth, set_trades

# Third party packages
# Local packages
from dccd.tools.io import IODataBase

__all__ = [
    'DownloadBitmexData', 'get_data_bitmex', 'get_orderbook_bitmex',
    'get_trades_bitmex',
]

# =========================================================================== #
#                              Parser functions                               #
# =========================================================================== #


def _parser_trades(tData: dict[str, Any], i: int = 0) -> dict[str, Any]:
    """Parse a single trade entry from a Bitmex WebSocket message.

    Parameters
    ----------
    tData : dict
        Raw trade dict from the Bitmex 'trade' table action.
        Expected keys: ``'timestamp'``, ``'price'``, ``'size'``, ``'side'``.
    i : int, optional
        Index used to make the ``tid`` unique within the same millisecond,
        default is 0.

    Returns
    -------
    dict
        Normalised trade with keys: ``'tid'``, ``'timestamp'``,
        ``'price'``, ``'amount'``, ``'type'``.

    """
    t = dt.strptime(tData['timestamp'], '%Y-%m-%dT%H:%M:%S.%f%z').timestamp()

    return {
        'tid': int(t * 1000 + i),
        'timestamp': int(t * 1000),
        'price': tData['price'],
        'amount': tData['size'],
        'type': tData['side'].lower(),
    }


def _parser_book(tData: dict[str, Any]) -> dict[str, Any] | int:
    """Parse a single order-book entry from a Bitmex WebSocket message.

    Parameters
    ----------
    tData : dict
        Raw order dict from the Bitmex 'orderBookL2' table action.
        Expected keys: ``'side'``, and optionally ``'price'`` and ``'size'``.

    Returns
    -------
    dict or int
        If ``'price'`` is present: ``{'amount': signed_size, 'price': price}``.
        Otherwise: signed size as int (positive for Buy, negative for Sell).

    """
    if tData['side'] == 'Buy':
        s = 1
    else:
        s = -1

    if 'price' in tData.keys():
        return {'amount': s * tData['size'], 'price': tData['price']}

    return s * tData['size']


# =========================================================================== #
#                              Download objects                               #
# =========================================================================== #


class DownloadBitmexData(ContinuousDownloader):
    """ Basis object to download data from a stream websocket client API.

    Parameters
    ----------
    time_step : int or None, optional
        Number of seconds between two snapshots of data, minimum is 1, default
        is 60 (one minute). Each ``time_step`` seconds data will be processed
        and pushed to the database.  Pass ``None`` to receive data tick-by-tick
        without periodic aggregation.
    until : int, optional
        Number of seconds before stopping, or a future Unix timestamp at which
        to stop.  Default is ``3600`` (one hour).

    Attributes
    ----------
    host : str
        Address of host to connect.
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
    __call__

    """

    def __init__(self, time_step: int = 60, until: int | None = 3600) -> None:
        """ Initialize object.

        Parameters
        ----------
        time_step : int or None, optional
            Snapshot interval in seconds.  Default is ``60``.
        until : int or None, optional
            Seconds to run, or a future Unix timestamp to stop at.
            Default is ``3600``.

        """
        stop: int
        if until is None:
            stop = 0
        elif until > time.time():
            stop = until - int(time.time())
        else:
            stop = until

        ContinuousDownloader.__init__(self, 'bitmex', time_step=time_step,
                                      STOP=stop)
        self._parser_data: dict[str, Any] = {
            'orderBookL2_25': self.parser_book,
            'trade': self.parser_trades,
        }
        self.d: dict[int, Any] = {}
        self.start = False

    def parser_book(self, data: dict[str, Any]) -> None:
        """ Parse and maintain a local copy of the order book.

        Handles ``partial`` (snapshot), ``insert``, ``update``, and ``delete``
        actions from the Bitmex WebSocket feed.

        Parameters
        ----------
        data : dict
            Order book message from the WebSocket API.  Must contain
            ``'action'`` and ``'data'`` keys.

        """
        action = data['action']

        for d in data['data']:
            if action == 'partial':
                self.d[d['id']] = _parser_book(d)
                self.start = True
            elif not self.start:
                self.logger.info("Waiting data")
                continue
            elif action == 'delete':
                self.d.pop(d['id'])
            elif action == 'insert':
                self.d[d['id']] = _parser_book(d)
            elif action == 'update':
                self.d[d['id']]['amount'] = _parser_book(d)
            else:
                self.logger.error('Unknown action {}: {}'.format(action, data))

        self._data[self.t] = {v['price']: v['amount'] for v in self.d.values()}  # type: ignore[assignment]

    def parser_trades(self, data: dict[str, Any]) -> None:
        """ Parse trade data and accumulate records for the current timestep.

        Parameters
        ----------
        data : dict
            Trade message from the WebSocket API.  Must contain a ``'data'``
            key with a list of trade records.

        """
        i, _data = 0, []
        for d in data['data']:
            _data += [_parser_trades(d, i)]
            i += 1

        if self.t in self._data.keys():
            self._data[self.t] += _data
        else:
            self._data[self.t] = _data

    async def on_message(self, data: dict[str, Any] | list[Any]) -> None:
        """ Route an incoming websocket message to the appropriate parser. """
        if isinstance(data, dict):
            if 'action' not in data.keys():
                self.logger.info('No action: {}'.format(data))
            else:
                self.parser(data)
        else:
            self.logger.error('Not recognizing: {}'.format(data))

    def __call__(self, *args: str) -> 'DownloadBitmexData':
        """ Open a websocket connection and save/update the database.

        Run asynchronously two loops to get data from Bitmex websocket and
        save/update the database.

        Parameters
        ----------
        *args : str
            Positional arguments joined with ``':'`` and passed as the
            ``args`` subscribe parameter.  The first element should be the
            channel name (e.g. ``'orderBookL2_25'`` or ``'trade'``) followed
            by any instrument symbol (e.g. ``'XBTUSD'``).

        Warnings
        --------
        '_raw' option not yet working for Bitmex.

        References
        ----------
        .. [1] https://www.bitmex.com/api/

        """
        self.parser = self.get_parser(args[0])

        self.logger.info('Try connect WS and set {} stream.'.format(args[0]))

        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(asyncio.gather(
            self._connect(args=':'.join(args)),
            self._loop()
        ))

        return self


# =========================================================================== #
#                            High level functions                             #
# =========================================================================== #


def get_data_bitmex(process_func: Any, *args: str, time_step: int = 60,
                    until: int | None = None, path: str | None = None,
                    save_method: str = 'dataframe', io_params: dict[str, Any] = {},
                    **kwargs: Any) -> None:
    """ Download data from Bitmex exchange and update the database.

    Parameters
    ----------
    process_func : callable
        Function to process and clean data before saving.  Must accept
        ``data`` as its first argument plus optional keyword arguments; see
        :mod:`dccd.process_data` for examples.
    *args : str
        Channel and optional instrument, e.g. ``'trade', 'XBTUSD'``.  Passed
        directly to :meth:`DownloadBitmexData.__call__`.
    time_step : int, optional
        Number of seconds between snapshots, default ``60`` (1 minute).
    until : int, optional
        Seconds to run, or a future Unix timestamp to stop at.  ``None`` or
        ``0`` means run indefinitely.
    path : str, optional
        Directory for the database.  Defaults to
        ``'database/bitmex/{channel}'``.
    save_method : {'DataFrame', 'SQLite', 'CSV', 'Excel', 'PostgreSQL',\
                   'Oracle', 'MSSQL', 'MySQL'}, optional
        Storage format for :class:`~dccd.tools.io.IODataBase`, default
        ``'dataframe'``.
    io_params : dict, optional
        Extra keyword arguments forwarded to the
        :class:`~dccd.tools.io.IODataBase` callable.
    **kwargs
        Additional keyword arguments forwarded to the websocket connector.

    Warnings
    --------
    '_raw' option not yet working for Bitmex.

    See Also
    --------
    process_data : helper functions to transform raw payloads.
    tools.io.IODataBase : persistence layer.

    References
    ----------
    .. [1] https://www.bitmex.com/api/

    """
    if path is None:
        path = 'database/bitmex/{}'.format(args[0])

    saver = IODataBase(path, method=save_method)
    downloader = DownloadBitmexData(time_step=time_step, until=until)
    downloader.set_process_data(process_func)
    downloader.set_saver(saver, **io_params)
    downloader(*args)


def get_orderbook_bitmex(*args: str, time_step: int = 60, until: int | None = None,
                         path: str | None = None, save_method: str = 'dataframe',
                         io_params: dict[str, Any] = {}) -> None:
    """ Download reconstructed order book from Bitmex exchange. """
    get_data_bitmex(set_marketdepth, *args, time_step=time_step, until=until,
                    path=path, save_method=save_method, io_params=io_params)


def get_trades_bitmex(*args: str, time_step: int = 60, until: int | None = None,
                      path: str | None = None, save_method: str = 'dataframe',
                      io_params: dict[str, Any] = {}) -> None:
    """ Download trades tick by tick from Bitmex exchange. """
    get_data_bitmex(set_trades, *args, time_step=time_step, until=until,
                    path=path, save_method=save_method, io_params=io_params)
