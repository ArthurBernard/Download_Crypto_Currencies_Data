#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2019-03-25 19:31:56
# @Last modified by: ArthurBernard
# @Last modified time: 2019-09-11 08:47:31

""" Objects and functions to download data from Bitfinex exchange.

.. currentmodule:: dccd.continuous_dl.bitfinex

These functions and objects allow you to continuously download data and update
your database.

High level API
--------------

.. autofunction:: get_data_bitfinex
.. autofunction:: get_orderbook_bitfinex
.. autofunction:: get_trades_bitfinex

Low level API
-------------

.. autoclass:: dccd.continuous_dl.bitfinex.DownloadBitfinexData
   :members: set_process_data, set_saver
   :special-members: __call__
   :show-inheritance:

"""

# Built-in packages
import asyncio
import logging
import time
from typing import Any

from dccd.continuous_dl.exchange import ContinuousDownloader
from dccd.process_data import set_marketdepth, set_ohlc, set_orders, set_trades

# Third party packages
# Local packages
from dccd.tools.io import IODataBase

__all__ = [
    'DownloadBitfinexData', 'get_data_bitfinex', 'get_orderbook_bitfinex',
    'get_trades_bitfinex',
]

# =========================================================================== #
#                              Parser functions                               #
# =========================================================================== #


def _parser_trades(tData: list[Any]) -> dict[str, Any]:
    if tData[1] == 'te':
            tData = tData[2]

    return {
        'tid': tData[0],
        'timestamp': tData[1] / 1000,
        'price': tData[3],
        'amount': abs(tData[2]),
        'type': 'buy' if tData[2] > 0. else 'sell',
    }


def _parser_book(tData: list[Any]) -> dict[str, Any]:
    if isinstance(tData[1], list):
        tData = tData[1]

    return {'price': str(tData[0]), 'count': tData[1], 'amount': tData[2]}


# =========================================================================== #
#                              Download objects                               #
# =========================================================================== #


class DownloadBitfinexData(ContinuousDownloader):
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
        if until is None:
            until = 0
        elif until > time.time():
            until -= int(time.time())

        ContinuousDownloader.__init__(self, 'bitfinex', time_step=time_step,
                                      STOP=until)

        self._parser_data: dict[str, Any] = {
            'book': self.parser_book,
            'book_raw': self.parser_raw_book,
            'trades': self.parser_trades,
            'trades_raw': self.parser_raw_trades,
        }
        self.logger = logging.getLogger(__name__)
        self.d: dict[str, Any] = {}

    def parser_raw_book(self, data: list[Any]) -> None:
        """ Parse raw order book, each timestep set in a list all orders.

        Parameters
        ----------
        data : list
            Order data.

        """
        parsed = _parser_book(data)
        self._raw_parser(parsed)

    def parser_book(self, data: list[Any]) -> None:
        """ Parse market depth of order book.

        Parameters
        ----------
        data : list
            Order data.

        """
        parsed = _parser_book(data)

        if parsed['count'] > 0:
            if parsed['price'] in self.d.keys():
                self.d[parsed['price']]['amount'] += parsed['amount']
            else:
                self.d[parsed['price']] = parsed
        else:
            self.d.pop(parsed['price'])

        self._data[self.t] = {v['price']: v['amount'] for v in self.d.values()}  # type: ignore[assignment]

    def parser_raw_trades(self, data: list[Any]) -> None:
        """ Parse raw trade data tick-by-tick.

        Parameters
        ----------
        data : list
            Trade data.

        """
        if data[1] == 'tu':
            return

        parsed = _parser_trades(data)
        self._raw_parser(parsed)

    def parser_trades(self, data: list[Any]) -> None:
        """ Parse trade data and aggregate into OHLCV snapshots.

        Parameters
        ----------
        data : list
            Trade data.

        """
        if data[1] == 'tu':
            return

        parsed = _parser_trades(data)
        self._raw_parser(parsed)

    async def on_message(self, data: dict[str, Any] | list[Any]) -> None:
        """ Route an incoming websocket message to the appropriate parser. """
        if isinstance(data, list):
            if data[1] == 'hb':
                self.logger.info('HeartBeat')
            elif isinstance(data[1][0], list):
                for d in data[1]:
                    self.parser(d)
            else:
                self.parser(data)
        else:
            self.logger.info('{}'.format(data))

    def __call__(self, channel: str, **kwargs: Any) -> 'DownloadBitfinexData':
        """ Open a websocket connection and save/update the database.

        Run asynchronously two loops to get data from bitfinex websocket and
        save/update the database.

        Parameters
        ----------
        channel : {'book', 'book_raw', 'trades', 'trades_raw'}
            Channel to get data, by default data will be aggregated (OHLC for
            'trades' and reconstructed orderbook for 'book'), add '_raw' to the
            `channel` to get raw data (trade tick by tick or each orders).
        **kwargs
            Any revelevant keyword arguments will be passed to the websocket
            connector, see API documentation [1]_ for more details.

        Warnings
        --------
        'book_raw' and 'trades_raw' can be very memory expensive.

        References
        ----------
        .. [1] https://docs.bitfinex.com/v2/docs/ws-public

        """
        self.parser = self.get_parser(channel)

        channel = channel[:-4] if channel[-4:] == '_raw' else channel

        self.logger.info('Try connect WS and set {} stream.'.format(channel))

        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(asyncio.gather(
            self._connect(channel=channel, **kwargs),
            self._loop()
        ))

        return self


# =========================================================================== #
#                            High level functions                             #
# =========================================================================== #


def get_data_bitfinex(channel: str, process_func: Any, process_params: dict[str, Any] = {},
                      save_method: str = 'dataframe', io_params: dict[str, Any] = {},
                      time_step: int = 60, until: int | None = None, path: str | None = None,
                      **kwargs: Any) -> None:
    """ Download data from Bitfinex exchange and update the database.

    Parameters
    ----------
    channel : str, {'book', 'book_raw', 'trades', 'trades_raw'}
        Websocket channel to get data, by default data will be aggregated (OHLC
        for 'trades' and reconstructed orderbook for 'book'), add '_raw' to the
        `channel` to get raw data (trade tick by tick or each orders).
    process_func : callable
        Function to process and clean data before to be saved. Must take `data`
        in arguments and can take any optional keywords arguments, cf function
        exemples in :mod:`dccd.process_data`.
    process_params : dict, optional
        Dictionary of the keyword arguments available to `process_func`, cf
        documentation into :mod:`dccd.process_data`.
    save_method : {'DataFrame', 'SQLite', 'CSV', 'Excel', 'PostgreSQL',\
                   'Oracle', 'MSSQL', 'MySQL'},
        It will create an IODataBase object to save/update the database in the
        specified format `save_method`, default is 'DataFrame' it save as
        binary pd.DataFrame object. More informations are available into
        :mod:`dccd.tools.io`.
    io_params : dict, optional
        Dictionary of the keyword arguments available to the
        ``dccd.tools.io.IODataBase`` callable method. Note: With SQL format
        some parameters are compulsory, see details into :mod:`dccd.tools.io`.
    time_step : int, optional
        Number of second between two snapshots of data, default 60 (1 minute).
    until : int, optional
        Number of seconds before stoping to download and update, default is
        None. If `until` equal 0 or None it means it never stop.
    path : str, optional
        Path to save/update the database, default is None. If `path` is None,
        database is saved at the relative path './database/bitfinex/`channel`'.
    **kwargs
        Any revelevant keyword arguments will be passed to the websocket
        connector, see Bitfinex API documentation [2]_ for more details.

    Warnings
    --------
    'book_raw' and 'trades_raw' can be very memory expensive.

    See Also
    --------
    process_data : function to process/clean data (set_marketdepth, set_ohlc,
        set_orders, set_marketdepth).
    tools.io.IODataBase : object to save/update the database with respect to
        specified format.

    References
    ----------
    .. [2] https://docs.bitfinex.com/v2/docs/ws-public

    """
    if path is None:
        path = './database/bitfinex/{}'.format(channel)

    saver = IODataBase(path, method=save_method)
    downloader = DownloadBitfinexData(time_step=time_step, until=until)
    downloader.set_process_data(process_func, **process_params)
    downloader.set_saver(saver, **io_params)
    downloader(channel, **kwargs)


def get_orders_bitfinex(symbol: str, precision: str = 'P0', frequency: str = 'F0',
                        lenght: str = '25', time_step: int = 60, until: int | None = None,
                        path: str | None = None, save_method: str = 'dataframe',
                        io_params: dict[str, Any] = {}) -> None:
    """ Download raw order data from Bitfinex exchange. """
    get_data_bitfinex('book_raw', set_orders, time_step=time_step, until=until,
                      path=path, save_method=save_method, io_params=io_params,
                      symbol=symbol, precision=precision, frequency=frequency,
                      lenght=lenght)


def get_orderbook_bitfinex(symbol: str, precision: str = 'P0', frequency: str = 'F0',
                           lenght: str = '25', time_step: int = 60, until: int | None = None,
                           path: str | None = None, save_method: str = 'dataframe',
                           io_params: dict[str, Any] = {}) -> None:
    """ Download reconstructed order book from Bitfinex exchange. """
    get_data_bitfinex('book', set_marketdepth, time_step=time_step,
                      until=until, path=path, save_method=save_method,
                      io_params=io_params, symbol=symbol, precision=precision,
                      frequency=frequency, lenght=lenght)


def get_trades_bitfinex(symbol: str, time_step: int = 60, until: int | None = None,
                        path: str | None = None, save_method: str = 'dataframe',
                        io_params: dict[str, Any] = {}) -> None:
    """ Download trades tick by tick from Bitfinex exchange. """
    get_data_bitfinex('trades_raw', set_trades, time_step=time_step,
                      until=until, path=path, save_method=save_method,
                      io_params=io_params, symbol=symbol)


def get_ohlc_bitfinex(symbol: str, time_step: int = 60, until: int | None = None,
                      path: str | None = None, save_method: str = 'dataframe',
                      io_params: dict[str, Any] = {}) -> None:
    """ Download OHLCV data from Bitfinex exchange. """
    get_data_bitfinex('trades', set_ohlc, time_step=time_step, until=until,
                      path=path, save_method=save_method, io_params=io_params,
                      process_params={'ts': time_step}, symbol=symbol)
