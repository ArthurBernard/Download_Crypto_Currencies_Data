#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2019-07-31 10:38:29
# @Last modified by: ArthurBernard
# @Last modified time: 2019-08-07 19:11:26

""" Connector objects to WebSockets API client to download data.

The following objects are shapped to download data from crypto-currency
exchanges (currently only bitfinex).

"""

# Built-in packages
import time
import logging
import json
import asyncio

# Third party packages
import websockets

# Local packages

__all__ = ['BasisWebSocket']


class BasisWebSocket:
    """ Basis object to connect at a specified stream to websocket client API.

    Attributes
    ----------
    host : str
        Adress of host to connect.
    conn_para : dict
        Parameters of websocket connection.
    ws : websockets.client.WebSocketClientProtocol
        Connection with the websocket client.
    is_connect : bool
        `True` if connected else `False`.

    Methods
    -------
    on_open(**kwargs)
        Method to connect to a stream of websocket client API.

    """
    ws = False
    is_connect = False

    def __init__(self, host, log_level='DEBUG', conn={}, subs={}):
        """ Initialize object.

        Parameters
        ----------
        host : str
            Adress of host to connect.
        log_level : str, optional
            Level of logging, default is 'INFO'.
        conn : dict
            Parameters to connection setting.
        subs : dict
            Data to subscribe to a stream.

        """
        # Set websocket variables
        self.host = host
        self.conn_para = conn  # {
        self.subs_data = subs

        # Set websocket connection indicators
        # self.ws = False
        # self.is_connect = False

        # Set logger
        self.logger = logging.getLogger(__name__)
        self.logger.info('Init websocket object.')

    async def _connect(self, **kwargs):
        """ Connection to websocket. """
        # Connect to host websocket
        async with websockets.connect(self.host, **self.conn_para) as self.ws:
            self.logger.info('Websocket connected to {}.'.format(self.host))

            # Subscribe to a stream
            await self._subscribe(**kwargs)
            await self.wait_that('is_connect')

            # Loop on received message
            try:
                async for msg in self.ws:
                    message = json.loads(msg)
                    await self.on_message(message)

                    # Stop if disconnect
                    if not self.is_connect:
                        return

            # Exit due to closed connection
            except websockets.exceptions.ConnectionClosed:
                await self.on_error(
                    'ConnectionClosed',
                    "Code is {}\n".format(self.ws.close_code),
                    "Reason is '{}'".format(self.ws.close_reason)
                )

    async def _subscribe(self, **kwargs):
        """ Connect to a stream. """
        # data = {"event": "subscribe", **kwargs}
        data = {**self.subs_data, **kwargs}

        self.logger.info('Subscription data: {}'.format(data))

        # Wait the connection
        await self.wait_that('ws')

        # Send data to subscribe
        await self.ws.send(json.dumps(data))
        self.is_connect = True

        return

    async def on_error(self, error, *args):
        """ On websocket error print and fire event. """
        self.logger.error(error + ': ' + ''.join(args))
        self.on_close()

    def on_close(self):
        """ On websocket close print and fire event. """
        self.logger.info("Websocket closed.")
        self.is_connect = False
        self.ws.close()

    def on_open(self, **kwargs):
        """ On websocket open.

        Parameters
        ----------
        kwargs : dict
            Any relevant parameters to connection.

        """
        self.logger.info("Websocket open.")
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.gather(
            self._connect(**kwargs),
        ))

    async def on_message(self, message):
        """ On websocket display message. """
        self.logger.info('Message: {}'.format(message))

    async def wait_that(self, is_true):
        """ Wait before running. """
        while not self.__getattribute__(is_true):
            self.logger.debug('Please wait that "{}".'.format(is_true))
            await asyncio.sleep(1)


class DownloadDataWebSocket(BasisWebSocket):
    """ Basis object to download data from a stream websocket client API.

    Attributes
    ----------
    host : str
        Adress of host to connect.
    conn_par : dict
        Parameters of websocket connection.
    ws : websockets.client.WebSocketClientProtocol
        Connection with the websocket client.
    is_connect : bool
        `True` if connected else `False`.
    ts : int
        Number of second between two observations.
    t : int
        Current timestamp but rounded by `ts`.
    until : int
        Timestamp to stop to download data.

    Methods
    -------
    on_open(**kwargs)
        Method to connect to a stream of websocket client API.

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

    def __init__(self, host, log_level='DEBUG', time_step=60, STOP=3600,
                 **kwargs):
        """ Initialize object.

        Parameters
        ----------
        host : str
            Name of an allowed exchange or url of the host exchange.
        time_step : int, optional
            Number of seconds between two observations, default is 60 (one
            minute).
        pasrer : function, optional
            Function to parse raw data.
        STOP : int, optional
            Number of seconds before stoping, default is 3600 (one hour).

        """
        if host.lower() in DownloadDataWebSocket._parser_exchange.keys():
            BasisWebSocket.__init__(self, **self._parser_exchange[host])

        else:
            BasisWebSocket.__init__(self, host, log_level=log_level, **kwargs)

        # Set variables
        self.ts = time_step
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

    # async def __aenter__(self):
    #    """ Enter async context manager. """
    #    return self

    # async def __aexit__(self, exc_type, exc_value, traceback):
    #    """ Exit async context manager. """
    #    txt = f'{exc_type}: {exc_value}\n{traceback}'
    #    self.logger.info(f'Exit context manager: {txt}')
    #    self.on_close()

    async def _loop(self):
        """ Update database. """
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
        """ Set data to order book. """
        # TODO : if time_step is None
        try:
            self._data[self.t] += [data]

        except KeyError:
            self._data[self.t] = [data]

    def _current_timestep(self):
        """ Set current time rounded by `timestep`. """
        return int((time.time() + 0.01) // self.ts * self.ts)

    def set_process_data(self, func, **kwargs):
        self.process_data = func
        self.process_params = kwargs

    def set_saver(self, func, **kwargs):
        self.saver = func
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
        if key not in self._parser_data.keys():
            return self._parser_debug

        else:
            return self._parser_data[key]
