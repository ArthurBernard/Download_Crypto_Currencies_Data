#!/usr/bin/env python3
# coding: utf-8

""" Objects and functions to download data from Kraken exchange (WebSocket).

.. currentmodule:: dccd.continuous_dl.kraken

High level API
--------------

.. autofunction:: get_data_kraken
.. autofunction:: get_orderbook_kraken
.. autofunction:: get_trades_kraken

Low level API
-------------

.. autoclass:: dccd.continuous_dl.kraken.DownloadKrakenData
   :members: set_process_data, set_saver
   :special-members: __call__
   :show-inheritance:

"""

# Built-in packages
import json
import logging
import time
from datetime import datetime, timezone

# Third party packages
# Local packages
from dccd.continuous_dl.exchange import ContinuousDownloader
from dccd.process_data import set_marketdepth, set_orders, set_trades
from dccd.tools.io import IODataBase

__all__ = [
    'DownloadKrakenData', 'get_data_kraken', 'get_orderbook_kraken',
    'get_trades_kraken',
]

_KRAKEN_WS_URL = 'wss://ws.kraken.com/v2'


def _iso_to_ts(iso: str) -> int:
    """ Convert an ISO 8601 timestamp string to a Unix timestamp (seconds). """
    return int(datetime.fromisoformat(iso.replace('Z', '+00:00'))
               .replace(tzinfo=timezone.utc).timestamp())


def _parser_trades(data: list[dict]) -> list[dict]:
    """ Parse a trade push message from Kraken WebSocket v2.

    Parameters
    ----------
    data : list of dict
        The ``data`` field of a Kraken trade push message.

    Returns
    -------
    list of dict
        Each dict has keys: ``tid``, ``timestamp``, ``price``, ``amount``,
        ``type`` ('buy' or 'sell').

    """
    return [{
        'tid': d['trade_id'],
        'timestamp': _iso_to_ts(d['timestamp']),
        'price': float(d['price']),
        'amount': float(d['qty']),
        'type': d['side'],
    } for d in data]


def _parser_book(data: list[dict]) -> dict[str, float]:
    """ Parse a book push message from Kraken WebSocket v2.

    Parameters
    ----------
    data : list of dict
        The ``data`` field of a Kraken book push message.
        Each element has ``bids`` and ``asks`` lists of
        ``{price, qty}`` dicts.

    Returns
    -------
    dict
        Price levels as strings; bids positive, asks prefixed with ``'-'``
        and negative. Zero quantity means the level was removed.

    """
    book: dict[str, float] = {}
    for snap in data:
        for bid in snap.get('bids', []):
            book[str(bid['price'])] = float(bid['qty'])
        for ask in snap.get('asks', []):
            book['-' + str(ask['price'])] = -float(ask['qty'])
    return book


def _parser_kline(data: list[dict]) -> list[dict]:
    """ Parse an ohlc push message from Kraken WebSocket v2.

    Parameters
    ----------
    data : list of dict
        The ``data`` field of a Kraken ohlc push message.

    Returns
    -------
    list of dict
        Each dict has keys: ``timestamp``, ``open``, ``high``, ``low``,
        ``close``, ``volume``.

    """
    return [{
        'timestamp': _iso_to_ts(d['interval_begin']),
        'open': float(d['open']),
        'high': float(d['high']),
        'low': float(d['low']),
        'close': float(d['close']),
        'volume': float(d['volume']),
    } for d in data]


class DownloadKrakenData(ContinuousDownloader):
    """ Download data continuously from Kraken via WebSocket v2.

    Parameters
    ----------
    pair : str
        Trading pair in Kraken format (e.g. 'BTC/USD').
    time_step : int, optional
        Seconds between data snapshots, default is 60.
    until : int, optional
        Seconds to run or stop timestamp, default is 3600.
    span : int, optional
        OHLCV interval in seconds; if given, also subscribes to the ohlc
        channel. Must be a multiple of 60. Default is None.

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

    def __init__(self, pair: str = 'BTC/USD', time_step: int = 60,
                 until: int | None = 3600, span: int | None = None) -> None:
        """ Initialize object. """
        if until is None:
            until = 0
        elif until > time.time():
            until -= int(time.time())

        self.pair = pair
        self._span = span
        ContinuousDownloader.__init__(
            self, _KRAKEN_WS_URL, time_step=time_step, STOP=until,
        )
        self._parser_data = {
            'trades': self.parser_trades,
            'book': self.parser_book,
            'kline': self.parser_kline,
        }
        self.logger = logging.getLogger(__name__)
        self.d: dict[str, float] = {}

    async def _subscribe(self, **kwargs: object) -> None:
        """ Send per-channel subscribe messages to Kraken WebSocket v2. """
        await self.wait_that('ws')
        await self.ws.send(json.dumps({
            'method': 'subscribe',
            'params': {'channel': 'trade', 'symbol': [self.pair]},
        }))
        await self.ws.send(json.dumps({
            'method': 'subscribe',
            'params': {'channel': 'book', 'symbol': [self.pair], 'depth': 50},
        }))
        if self._span is not None:
            period = max(1, self._span // 60)
            await self.ws.send(json.dumps({
                'method': 'subscribe',
                'params': {'channel': 'ohlc', 'symbol': [self.pair], 'period': period},
            }))
        self.is_connect = True

    async def on_message(self, msg: dict) -> None:
        """ Dispatch incoming Kraken WebSocket v2 push messages.

        Parameters
        ----------
        msg : dict
            Kraken push message with ``channel`` and ``data`` fields.

        """
        channel = msg.get('channel', '')
        msg_type = msg.get('type', '')
        if msg_type in ('heartbeat', 'pong') or channel in ('heartbeat', 'status'):
            return
        if channel == 'trade':
            self.parser_trades(msg.get('data', []))
        elif channel == 'book':
            self.parser_book(msg)
        elif channel == 'ohlc':
            self.parser_kline(msg.get('data', []))

    def parser_trades(self, data: list[dict]) -> None:
        """ Parse and store a trade push message.

        Parameters
        ----------
        data : list of dict
            The ``data`` field from the Kraken trade push message.

        """
        for trade in _parser_trades(data):
            self._raw_parser(trade)

    def parser_book(self, msg: dict) -> None:
        """ Parse and update the order book from a book push message.

        Parameters
        ----------
        msg : dict
            Full Kraken book push message (contains ``type`` and ``data``).

        """
        updates = _parser_book(msg.get('data', []))
        for price, qty in updates.items():
            if qty == 0:
                self.d.pop(price, None)
            else:
                self.d[price] = qty
        self._data[self.t] = dict(self.d)

    def parser_kline(self, data: list[dict]) -> None:
        """ Parse and store an ohlc push message.

        Parameters
        ----------
        data : list of dict
            The ``data`` field from the Kraken ohlc push message.

        """
        for candle in _parser_kline(data):
            self._raw_parser(candle)

    def _raw_parser(self, data: object) -> None:
        if self.t not in self._data:
            self._data[self.t] = []
        self._data[self.t].append(data)  # type: ignore[union-attr]


def get_trades_kraken(path: str, pair: str = 'BTC/USD', time_step: int = 60,
                      until: int = 3600, form: str = 'csv') -> None:
    """ Download trades data from Kraken via WebSocket.

    Parameters
    ----------
    path : str
        Path to save data.
    pair : str, optional
        Trading pair in Kraken format (e.g. 'BTC/USD'), default is 'BTC/USD'.
    time_step : int, optional
        Seconds between snapshots, default is 60.
    until : int, optional
        Duration in seconds or stop timestamp, default is 3600.
    form : str, optional
        Save format ('csv', 'parquet', etc.), default is 'csv'.

    """
    downloader = DownloadKrakenData(pair=pair, time_step=time_step, until=until)
    downloader.set_process_data(set_trades)
    downloader.set_saver(IODataBase(path, method=form))
    downloader(pair=pair)


def get_orderbook_kraken(path: str, pair: str = 'BTC/USD', time_step: int = 60,
                         until: int = 3600, form: str = 'csv') -> None:
    """ Download order book data from Kraken via WebSocket.

    Parameters
    ----------
    path : str
        Path to save data.
    pair : str, optional
        Trading pair in Kraken format (e.g. 'BTC/USD'), default is 'BTC/USD'.
    time_step : int, optional
        Seconds between snapshots, default is 60.
    until : int, optional
        Duration in seconds or stop timestamp, default is 3600.
    form : str, optional
        Save format ('csv', 'parquet', etc.), default is 'csv'.

    """
    downloader = DownloadKrakenData(pair=pair, time_step=time_step, until=until)
    downloader.set_process_data(set_marketdepth)
    downloader.set_saver(IODataBase(path, method=form))
    downloader(pair=pair)


def get_data_kraken(path: str, pair: str = 'BTC/USD', time_step: int = 60,
                    until: int = 3600, form: str = 'csv') -> None:
    """ Download order book and trades data from Kraken via WebSocket.

    Parameters
    ----------
    path : str
        Path to save data.
    pair : str, optional
        Trading pair in Kraken format (e.g. 'BTC/USD'), default is 'BTC/USD'.
    time_step : int, optional
        Seconds between snapshots, default is 60.
    until : int, optional
        Duration in seconds or stop timestamp, default is 3600.
    form : str, optional
        Save format ('csv', 'parquet', etc.), default is 'csv'.

    """
    downloader = DownloadKrakenData(pair=pair, time_step=time_step, until=until)
    downloader.set_process_data(set_orders)
    downloader.set_saver(IODataBase(path, method=form))
    downloader(pair=pair)
