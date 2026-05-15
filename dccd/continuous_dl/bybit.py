#!/usr/bin/env python3
# coding: utf-8

""" Objects and functions to download data from Bybit exchange (WebSocket).

.. currentmodule:: dccd.continuous_dl.bybit

High level API
--------------

.. autofunction:: get_data_bybit
.. autofunction:: get_orderbook_bybit
.. autofunction:: get_trades_bybit

Low level API
-------------

.. autoclass:: dccd.continuous_dl.bybit.DownloadBybitData
   :members: set_process_data, set_saver
   :special-members: __call__
   :show-inheritance:

"""

# Built-in packages
import logging
import time

# Third party packages
# Local packages
from dccd.continuous_dl.exchange import ContinuousDownloader
from dccd.process_data import set_marketdepth, set_orders, set_trades
from dccd.tools.io import IODataBase

__all__ = [
    'DownloadBybitData', 'get_data_bybit', 'get_orderbook_bybit',
    'get_trades_bybit',
]

_BYBIT_WS_URL = 'wss://stream.bybit.com/v5/public/spot'


def _parser_trades(msg):
    """Parse a publicTrade message from Bybit WebSocket v5.

    Parameters
    ----------
    msg : dict
        Raw message with a ``'data'`` list of trade dicts.
        Each trade dict contains: ``'i'`` (id), ``'T'`` (timestamp ms),
        ``'p'`` (price), ``'v'`` (volume), ``'S'`` (side 'Buy'/'Sell').

    Returns
    -------
    list of dict
        Each dict has keys: ``'tid'``, ``'timestamp'``, ``'price'``,
        ``'amount'``, ``'type'`` ('buy' or 'sell').

    """
    return [{
        'tid': int(d['i']),
        'timestamp': int(d['T']) / 1000,
        'price': float(d['p']),
        'amount': float(d['v']),
        'type': 'buy' if d['S'] == 'Buy' else 'sell',
    } for d in msg.get('data', [])]


def _parser_book(msg):
    """Parse an orderbook message from Bybit WebSocket v5.

    Parameters
    ----------
    msg : dict
        Raw message with a ``'data'`` dict containing ``'b'`` (bids) and
        ``'a'`` (asks), each a list of ``[price_str, qty_str]``.

    Returns
    -------
    dict
        Unified book dict: bid prices as positive float values keyed by the
        price string, ask prices prefixed with ``'-'`` as negative float values.

    """
    data = msg.get('data', {})
    book = {}
    for bid in data.get('b', []):
        book[bid[0]] = float(bid[1])
    for ask in data.get('a', []):
        book['-' + ask[0]] = -float(ask[1])
    return book


class DownloadBybitData(ContinuousDownloader):
    """ Download data continuously from Bybit via WebSocket v5.

    Parameters
    ----------
    pair : str
        Trading pair symbol (e.g. 'BTCUSDT').
    time_step : int, optional
        Seconds between data snapshots, default is 60.
    until : int, optional
        Seconds to run or stop timestamp, default is 3600.

    Attributes
    ----------
    host : str
        WebSocket URL.
    is_connect : bool
        True if connected.
    ts : int
        Snapshot interval in seconds.
    until : int
        Stop timestamp.

    Methods
    -------
    set_process_data
    set_saver
    __call__

    """

    def __init__(self, pair='BTCUSDT', time_step=60, until=3600):
        """ Initialize object. """
        if until is None:
            until = 0
        elif until > time.time():
            until -= int(time.time())

        self.pair = pair
        ContinuousDownloader.__init__(
            self, _BYBIT_WS_URL, time_step=time_step, STOP=until,
            subs={'op': 'subscribe',
                  'args': [f'publicTrade.{pair}', f'orderbook.50.{pair}']},
        )
        self._parser_data = {
            'trades': self.parser_trades,
            'book': self.parser_book,
        }
        self.logger = logging.getLogger(__name__)
        self.d = {}

    async def on_message(self, msg):
        """ Dispatch incoming WebSocket messages. """
        topic = msg.get('topic', '')
        if topic.startswith('publicTrade'):
            self.parser_trades(msg)
        elif topic.startswith('orderbook'):
            self.parser_book(msg)

    def parser_trades(self, msg):
        """ Parse and store trade messages.

        Parameters
        ----------
        msg : dict
            Raw WebSocket trade message.

        """
        for trade in _parser_trades(msg):
            self._raw_parser(trade)

    def parser_book(self, msg):
        """ Parse and update order book from WebSocket messages.

        Parameters
        ----------
        msg : dict
            Raw WebSocket orderbook message.

        """
        updates = _parser_book(msg)
        for price, qty in updates.items():
            if qty == 0:
                self.d.pop(price, None)
            else:
                self.d[price] = qty
        self._data[self.t] = dict(self.d)

    def _raw_parser(self, data):
        if self.t not in self._data:
            self._data[self.t] = []
        self._data[self.t].append(data)


def get_trades_bybit(path, pair='BTCUSDT', time_step=60, until=3600,
                     form='csv'):
    """ Download trades data from Bybit.

    Parameters
    ----------
    path : str
        Path to save data.
    pair : str, optional
        Trading pair, default is 'BTCUSDT'.
    time_step : int, optional
        Seconds between snapshots, default is 60.
    until : int, optional
        Duration in seconds, default is 3600.
    form : str, optional
        Save format, default is 'csv'.

    """
    downloader = DownloadBybitData(pair=pair, time_step=time_step, until=until)
    downloader.set_process_data(set_trades)
    downloader.set_saver(IODataBase(path, method=form))
    downloader(pair=pair)


def get_orderbook_bybit(path, pair='BTCUSDT', time_step=60, until=3600,
                        form='csv'):
    """ Download order book data from Bybit.

    Parameters
    ----------
    path : str
        Path to save data.
    pair : str, optional
        Trading pair, default is 'BTCUSDT'.
    time_step : int, optional
        Seconds between snapshots, default is 60.
    until : int, optional
        Duration in seconds, default is 3600.
    form : str, optional
        Save format, default is 'csv'.

    """
    downloader = DownloadBybitData(pair=pair, time_step=time_step, until=until)
    downloader.set_process_data(set_marketdepth)
    downloader.set_saver(IODataBase(path, method=form))
    downloader(pair=pair)


def get_data_bybit(path, pair='BTCUSDT', time_step=60, until=3600, form='csv'):
    """ Download order book and trades data from Bybit.

    Parameters
    ----------
    path : str
        Path to save data.
    pair : str, optional
        Trading pair, default is 'BTCUSDT'.
    time_step : int, optional
        Seconds between snapshots, default is 60.
    until : int, optional
        Duration in seconds, default is 3600.
    form : str, optional
        Save format, default is 'csv'.

    """
    downloader = DownloadBybitData(pair=pair, time_step=time_step, until=until)
    downloader.set_process_data(set_orders)
    downloader.set_saver(IODataBase(path, method=form))
    downloader(pair=pair)
