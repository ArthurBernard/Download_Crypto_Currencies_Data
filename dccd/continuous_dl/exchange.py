#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2019-08-30 09:28:43
# @Last modified by: ArthurBernard
# @Last modified time: 2019-08-30 10:01:33

""" Basis object to download continuously data from websocket.

Notes
-----
The following objects are shapped to download data from crypto-currency
exchanges (currently only bitfinex and bitmex).

"""

# Built-in packages
import time
import asyncio

# Third party packages

# Local packages
from dccd.tools.websocket import BasisWebSocket

__all__ = ['ContinuousDownloader']


class ContinuousDownloader(BasisWebSocket):
    """ Basis object to download data from a stream websocket client API.

    Parameters
    ----------
    host : {url, 'binance', 'bitfinex', 'bitmex'}
        Name of an allowed exchange or url of the host exchange. If url of
        a host exchange is provided, keyword arguments for connection and
        subscribe parameters must be also specified.
    time_step : int, optional
        Number of seconds between two snapshots of data, minimum is 1,
        default is 60 (one minute). Each `time_step` data will be
        processed and updated to the database.
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

    TODO :
    - None time_step send tick by tick data
    - Clean private/public methods
    - Add optional setting parser

    """

    _parser_exchange = {
        'binance': {
            'host': 'wss://stream.binance.com:9443/ws',
            'subs': {},  # {'stream': None},
        },
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
    _parser_data = {}

    def __init__(self, host, time_step=60, STOP=3600, **kwargs):
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
        self._data = {}

    def __aiter__(self):
        """ Set iterative method. """
        self.logger.debug('Starting generator websocket')

        return self

    async def __anext__(self):
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

        if t in self._data.keys():

            return self._data.pop(t)

        else:

            return None

    async def _loop(self):
        """ Loop to process and save data into database. """
        await self.wait_that('_data')

        async for data in self:
            self.logger.debug('Get data.')

            # Continue if no data received
            if data is None:
                self.logger.debug('No data')

                continue

            # Set DataFrame
            df = self.process_data(data, **self.process_params)

            # Update database
            self.saver(df, **self.io_params)

            self.logger.debug('Processed data:\n' + str(df))
            self.logger.debug('Catch data of TS : {}'.format(self.t))

            if not self.is_connect:
                self.logger.info('End to import data from Bitfinex.\n')

                return

    async def on_message(self, data):
        """ Parse any data received from the websocket. """
        self._raw_parser(data)

    def _raw_parser(self, data):
        # Set all data to a list
        if self.t in self._data.keys():
            self._data[self.t] += [data]

        else:
            self._data[self.t] = [data]

    def _current_timestep(self):
        """ Set current time rounded by `timestep`. """
        return int((time.time() + 0.001) // self.ts * self.ts)

    def set_process_data(self, func, **kwargs):
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

    def set_saver(self, call, **kwargs):
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

    def _parser_debug(self, data, level=0):
        # Function to debug and understand data structure
        if level == 0:
            self._data[self.t] = data

        if isinstance(data, list):
            self.logger.debug('Data is a list')
            for d in data:
                self._parser_debug(d, level=level + 1)

        elif isinstance(data, dict):
            self.logger.debug('Data is a dict')
            for k, a in data.items():
                self.logger.debug('{}: {}'.format(k, a))

        else:
            self.logger.debug('Data is {}: {}'.format(type(data), data))

    def get_parser(self, key):
        """ Get allowed data parser.

        Parameters
        ----------
        key : str
            Name code of data to parse. If `key` is not allowed then return a
            debug_parser which will display data structure.

        Returns
        -------
        function
            The allowed function to parse this kind of data.

        """
        if key not in self._parser_data.keys():
            self.logger.error('Unknown parser key {}, only {} allowed.'.format(
                key, self._parser_data.keys()
            ))

            return self._parser_debug

        return self._parser_data[key]
