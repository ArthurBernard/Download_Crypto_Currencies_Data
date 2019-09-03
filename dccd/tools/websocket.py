#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2019-07-31 10:38:29
# @Last modified by: ArthurBernard
# @Last modified time: 2019-08-30 09:59:54

""" Connector objects to WebSockets API client to download data. """

# Built-in packages
import logging
import json
import asyncio

# Third party packages
import websockets

# Local packages

__all__ = ['BasisWebSocket']

# =========================================================================== #
#                                Basis objects                                #
# =========================================================================== #


class BasisWebSocket:
    """ Basis object to connect at a specified stream to websocket client API.

    Parameters
    ----------
    host : str
        Adress of host to connect.
    conn : dict
        Parameters to connection setting.
    subs : dict
        Data to subscribe to a stream.

    Attributes
    ----------
    host : str
        Adress of host to connect.
    conn_para : dict
        Parameters of websocket connection.
    ws : websockets.client.WebSocketClientProtocol
        Connection with the websocket client.
    is_connect : bool
        - True if connected.
        - False`otherwise.

    Methods
    -------
    on_open

    """

    ws = False
    is_connect = False

    def __init__(self, host, conn={}, subs={}):
        """ Initialize object. """
        # Set websocket variables
        self.host = host
        self.conn_para = conn
        self.subs_data = subs

        # Set logger
        self.logger = logging.getLogger(__name__)
        self.logger.info('Init websocket object.')

    async def _connect(self, **kwargs):
        """ Connect to websocket. """
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
        **kwargs
            Any relevant keyword arguments to set connection.

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


