#!/usr/bin/env python3
# coding: utf-8

""" Objects and functions to download data from Binance exchange (WebSocket).

.. currentmodule:: dccd.continuous_dl.binance

High level API
--------------

.. autofunction:: get_data_binance
.. autofunction:: get_orderbook_binance
.. autofunction:: get_trades_binance

Low level API
-------------

.. autoclass:: dccd.continuous_dl.binance.DownloadBinanceData
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
    'DownloadBinanceData', 'get_data_binance', 'get_orderbook_binance',
    'get_trades_binance',
]

_BINANCE_WS_URL = 'wss://stream.binance.com:9443/stream?streams={sym}@trade/{sym}@depth50@100ms'


def _parser_trades(data: dict) -> list[dict]:
    """ Parse a trade message from Binance combined stream.

    Parameters
    ----------
    data : dict
        The ``data`` field of a combined-stream trade message.

    Returns
    -------
    list of dict
        Each dict has keys: ``tid``, ``timestamp``, ``price``, ``amount``,
        ``type`` ('buy' or 'sell').

    """
    return [{
        'tid': data['t'],
        'timestamp': int(data['T']) / 1000,
        'price': float(data['p']),
        'amount': float(data['q']),
        'type': 'sell' if data['m'] else 'buy',
    }]


def _parser_book(data: dict) -> dict:
    """ Parse a depth message from Binance combined stream.

    Parameters
    ----------
    data : dict
        The ``data`` field of a combined-stream depth message.

    Returns
    -------
    dict
        Price levels as strings; bids positive, asks prefixed with ``'-'``
        and negative. Zero quantity means the level was removed.

    """
    book: dict[str, float] = {}
    for bid in data.get('b', []):
        book[bid[0]] = float(bid[1])
    for ask in data.get('a', []):
        book['-' + ask[0]] = -float(ask[1])
    return book


class DownloadBinanceData(ContinuousDownloader):
    """ Download data continuously from Binance via combined WebSocket streams.

    Parameters
    ----------
    pair : str
        Trading pair symbol in Binance format (e.g. 'BTCUSDT').
    time_step : int, optional
        Seconds between data snapshots, default is 60.
    until : int, optional
        Seconds to run or stop timestamp, default is 3600.

    Attributes
    ----------
    host : str
        WebSocket URL (combined stream endpoint).
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

    def __init__(self, pair: str = 'BTCUSDT', time_step: int = 60,
                 until: int | None = 3600) -> None:
        """ Initialize object. """
        if until is None:
            until = 0
        elif until > time.time():
            until -= int(time.time())

        self.pair = pair
        url = _BINANCE_WS_URL.format(sym=pair.lower())
        ContinuousDownloader.__init__(self, url, time_step=time_step, STOP=until)
        self._parser_data = {
            'trades': self.parser_trades,
            'book': self.parser_book,
        }
        self.logger = logging.getLogger(__name__)
        self.d: dict[str, float] = {}

    async def _subscribe(self, **kwargs: object) -> None:
        """ Wait for connection; Binance streams are declared in the URL. """
        await self.wait_that('ws')
        self.is_connect = True

    async def on_message(self, msg: dict) -> None:
        """ Dispatch incoming combined-stream WebSocket messages.

        Parameters
        ----------
        msg : dict
            Combined-stream envelope with ``stream`` and ``data`` keys.

        """
        stream = msg.get('stream', '')
        if '@trade' in stream:
            self.parser_trades(msg['data'])
        elif '@depth' in stream:
            self.parser_book(msg['data'])

    def parser_trades(self, data: dict) -> None:
        """ Parse and store a trade message.

        Parameters
        ----------
        data : dict
            The ``data`` field from the combined-stream trade envelope.

        """
        for trade in _parser_trades(data):
            self._raw_parser(trade)

    def parser_book(self, data: dict) -> None:
        """ Parse and update the order book from a depth message.

        Parameters
        ----------
        data : dict
            The ``data`` field from the combined-stream depth envelope.

        """
        updates = _parser_book(data)
        for price, qty in updates.items():
            if qty == 0:
                self.d.pop(price, None)
            else:
                self.d[price] = qty
        self._data[self.t] = dict(self.d)

    def _raw_parser(self, data: object) -> None:
        if self.t not in self._data:
            self._data[self.t] = []
        self._data[self.t].append(data)  # type: ignore[union-attr]


def get_trades_binance(path: str, pair: str = 'BTCUSDT', time_step: int = 60,
                       until: int = 3600, form: str = 'csv') -> None:
    """ Download trades data from Binance via WebSocket.

    Parameters
    ----------
    path : str
        Path to save data.
    pair : str, optional
        Trading pair in Binance format (e.g. 'BTCUSDT'), default is 'BTCUSDT'.
    time_step : int, optional
        Seconds between snapshots, default is 60.
    until : int, optional
        Duration in seconds or stop timestamp, default is 3600.
    form : str, optional
        Save format ('csv', 'parquet', etc.), default is 'csv'.

    """
    downloader = DownloadBinanceData(pair=pair, time_step=time_step, until=until)
    downloader.set_process_data(set_trades)
    downloader.set_saver(IODataBase(path, method=form))
    downloader(pair=pair)


def get_orderbook_binance(path: str, pair: str = 'BTCUSDT', time_step: int = 60,
                          until: int = 3600, form: str = 'csv') -> None:
    """ Download order book data from Binance via WebSocket.

    Parameters
    ----------
    path : str
        Path to save data.
    pair : str, optional
        Trading pair in Binance format (e.g. 'BTCUSDT'), default is 'BTCUSDT'.
    time_step : int, optional
        Seconds between snapshots, default is 60.
    until : int, optional
        Duration in seconds or stop timestamp, default is 3600.
    form : str, optional
        Save format ('csv', 'parquet', etc.), default is 'csv'.

    """
    downloader = DownloadBinanceData(pair=pair, time_step=time_step, until=until)
    downloader.set_process_data(set_marketdepth)
    downloader.set_saver(IODataBase(path, method=form))
    downloader(pair=pair)


def get_data_binance(path: str, pair: str = 'BTCUSDT', time_step: int = 60,
                     until: int = 3600, form: str = 'csv') -> None:
    """ Download order book and trades data from Binance via WebSocket.

    Parameters
    ----------
    path : str
        Path to save data.
    pair : str, optional
        Trading pair in Binance format (e.g. 'BTCUSDT'), default is 'BTCUSDT'.
    time_step : int, optional
        Seconds between snapshots, default is 60.
    until : int, optional
        Duration in seconds or stop timestamp, default is 3600.
    form : str, optional
        Save format ('csv', 'parquet', etc.), default is 'csv'.

    """
    downloader = DownloadBinanceData(pair=pair, time_step=time_step, until=until)
    downloader.set_process_data(set_orders)
    downloader.set_saver(IODataBase(path, method=form))
    downloader(pair=pair)
