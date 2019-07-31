#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2019-07-31 10:38:29
# @Last modified by: ArthurBernard
# @Last modified time: 2019-07-31 12:18:20

""" Connector objects to WebSockets API client to download data.

The following objects are shapped to download data from crypto-currency
exchanges (currently only bitfinex).

"""

# Built-in packages
import time
import logging
import json
import asyncio

# External packages
import websockets

# Local packages


__all__ = ['BasisWebSocket']


class BasisWebSocket:
    """ Basis object to connect at a specified channel to websocket client API.

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

    Methods
    -------
    on_open(channel, **kwargs)
        Method to connect at a channel of websocket client API.

    """

    def __init__(self, host, log_level='DEBUG', ping_interval=5,
                 ping_timeout=5, close_timeout=5):
        """ Initialize object.

        Parameters
        ----------
        host : str
            Adress of host to connect.
        log_level : str, optional
            Level of logging, default is 'INFO'.

        """
        # Set websocket variables
        self.host = host
        self.conn_par = {
            'ping_interval': ping_interval,
            'ping_timeout': ping_timeout,
            'close_timeout': close_timeout,
        }

        # Set websocket connection indicators
        self.ws = False
        self.is_connect = False

        # Set logger
        self.logger = logging.getLogger(__name__)
        self.logger.info('Init websocket object.')

    async def _connect(self, channel, **kwargs):
        """ Connection to websocket. """
        # Connect to host websocket
        async with websockets.connect(self.host, **self.conn_par) as self.ws:
            self.logger.info('Websocket connected to {}.'.format(self.host))

            # Connect to channel
            await self._channel_connect(channel, **kwargs)
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

    async def _channel_connect(self, channel, **kwargs):
        """ Connect to a channel. """
        data = {"event": "subscribe", "channel": channel, **kwargs}
        self.logger.info(data)

        # Wait the connection
        await self.wait_that('ws')

        # Send data to channel connection
        await self.ws.send(json.dumps(data))
        self.is_connect = True
        self.logger.info('Set {} connection'.format(channel))

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

    def on_open(self, channel, **kwargs):
        """ On websocket open.

        Parameters
        ----------
        channel : str
            Channel to connect.
        kwargs : dict
            Any relevant parameters to connection.

        """
        self.logger.info("Websocket open.")
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.gather(
            self._connect(channel, **kwargs),
        ))

    async def on_message(self, message):
        """ On websocket display message. """
        self.logger.info('Message: {}'.format(message))

    async def wait_that(self, is_true):
        """ Wait before running. """
        while not self.__getattribute__(is_true):
            self.logger.debug('Please wait that "{}".'.format(is_true))
            await asyncio.sleep(1)
