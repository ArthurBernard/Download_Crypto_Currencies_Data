#!/usr/bin/env python3
# coding: utf-8

""" Objects and functions to download data from OKX exchange (WebSocket).

.. currentmodule:: dccd.continuous_dl.okx

High level API
--------------

.. autofunction:: get_data_okx
.. autofunction:: get_orderbook_okx
.. autofunction:: get_trades_okx

Low level API
-------------

.. autoclass:: dccd.continuous_dl.okx.DownloadOKXData
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
from dccd.tools.io import IODataBase

__all__ = [
    'DownloadOKXData', 'get_data_okx', 'get_orderbook_okx', 'get_trades_okx',
]

_OKX_WS_URL = 'wss://ws.okx.com:8443/ws/v5/public'

_OKX_WS_INTERVALS: dict[int, str] = {
    60: '1m', 300: '5m', 900: '15m', 1800: '30m',
    3600: '1H', 7200: '2H', 14400: '4H', 21600: '6H', 43200: '12H',
    86400: '1D', 604800: '1W',
}


def _okx_ws_interval(span: int) -> str:
    """ Convert a span in seconds to an OKX WebSocket candle channel suffix. """
    try:
        return _OKX_WS_INTERVALS[span]
    except KeyError:
        raise ValueError(
            f"Unsupported OKX WebSocket interval: {span}s. "
            f"Supported: {list(_OKX_WS_INTERVALS)}"
        )


def _parser_trades(data: list[dict]) -> list[dict]:
    """ Parse a trades message from OKX WebSocket.

    Parameters
    ----------
    data : list of dict
        The ``data`` field of an OKX trades push message.

    Returns
    -------
    list of dict
        Each dict has keys: ``tid``, ``timestamp``, ``price``, ``amount``,
        ``type`` ('buy' or 'sell').

    """
    return [{
        'tid': int(d['tradeId']),
        'timestamp': int(d['ts']) / 1000,
        'price': float(d['px']),
        'amount': float(d['sz']),
        'type': d['side'],
    } for d in data]


def _parser_book(data: list[dict]) -> dict[str, float]:
    """ Parse a books message from OKX WebSocket.

    Parameters
    ----------
    data : list of dict
        The ``data`` field of an OKX books push message.
        Each element has ``bids`` and ``asks`` arrays of
        ``[price, size, ...]`` entries.

    Returns
    -------
    dict
        Price levels as strings; bids positive, asks prefixed with ``'-'``
        and negative. Zero size means the level was removed.

    """
    book: dict[str, float] = {}
    for snap in data:
        for bid in snap.get('bids', []):
            book[bid[0]] = float(bid[1])
        for ask in snap.get('asks', []):
            book['-' + ask[0]] = -float(ask[1])
    return book


def _parser_kline(data: list[list]) -> list[dict]:
    """ Parse a candle message from OKX WebSocket.

    Parameters
    ----------
    data : list of list
        The ``data`` field of an OKX candle push message.
        Each row: ``[ts_ms, open, high, low, close, vol, ...]``.

    Returns
    -------
    list of dict
        Each dict has keys: ``timestamp``, ``open``, ``high``, ``low``,
        ``close``, ``volume``.

    """
    return [{
        'timestamp': int(row[0]) / 1000,
        'open': float(row[1]),
        'high': float(row[2]),
        'low': float(row[3]),
        'close': float(row[4]),
        'volume': float(row[5]),
    } for row in data]


class DownloadOKXData(ContinuousDownloader):
    """ Download data continuously from OKX via WebSocket v5.

    Parameters
    ----------
    pair : str
        Trading pair in OKX format (e.g. 'BTC-USDT').
    time_step : int, optional
        Seconds between data snapshots, default is 60.
    until : int, optional
        Seconds to run or stop timestamp, default is 3600.
    span : int, optional
        Candle interval in seconds; if given, subscribes to the candle channel
        in addition to trades and book. Default is None.

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

    def __init__(self, pair: str = 'BTC-USDT', time_step: int = 60,
                 until: int | None = 3600, span: int | None = None,
                 checkpoint_dir: str | None = None) -> None:
        """ Initialize object. """
        if until is None:
            until = 0
        elif until > time.time():
            until -= int(time.time())

        self.pair = pair
        args: list[dict] = [
            {'channel': 'trades', 'instId': pair},
            {'channel': 'books50-l2-tbt', 'instId': pair},
        ]
        if span is not None:
            args.append({'channel': f'candle{_okx_ws_interval(span)}', 'instId': pair})

        ContinuousDownloader.__init__(
            self, _OKX_WS_URL, time_step=time_step, STOP=until,
            checkpoint_dir=checkpoint_dir,
            subs={'op': 'subscribe', 'args': args},
        )
        self._parser_data = {
            'trades': self.parser_trades,
            'book': self.parser_book,
            'kline': self.parser_kline,
        }
        self.logger = logging.getLogger(__name__)
        self._load_checkpoint()

    async def on_message(self, msg: dict) -> None:
        """ Dispatch incoming OKX WebSocket push messages.

        Parameters
        ----------
        msg : dict
            OKX push message with ``arg`` and ``data`` fields.

        """
        channel = msg.get('arg', {}).get('channel', '')
        if channel == 'trades':
            self.parser_trades(msg['data'])
        elif channel.startswith('books'):
            self.parser_book(msg)
        elif channel.startswith('candle'):
            self.parser_kline(msg['data'])

    def parser_trades(self, data: list[dict]) -> None:
        """ Parse and store a trades push message.

        Parameters
        ----------
        data : list of dict
            The ``data`` field from the OKX trades push message.

        """
        self._push_trades(_parser_trades(data))

    def parser_book(self, msg: dict) -> None:
        """ Parse and update the order book from a books push message.

        Parameters
        ----------
        msg : dict
            Full OKX books push message (contains ``action`` and ``data``).

        """
        self._push_book_updates(_parser_book(msg.get('data', [])))

    def parser_kline(self, data: list[list]) -> None:
        """ Parse and store a candle push message.

        Parameters
        ----------
        data : list of list
            The ``data`` field from the OKX candle push message.

        """
        for candle in _parser_kline(data):
            self._raw_parser(candle)

def get_trades_okx(path: str, pair: str = 'BTC-USDT', time_step: int = 60,
                   until: int = 3600, form: str = 'csv') -> None:
    """ Download trades data from OKX via WebSocket.

    Parameters
    ----------
    path : str
        Path to save data.
    pair : str, optional
        Trading pair in OKX format (e.g. 'BTC-USDT'), default is 'BTC-USDT'.
    time_step : int, optional
        Seconds between snapshots, default is 60.
    until : int, optional
        Duration in seconds or stop timestamp, default is 3600.
    form : str, optional
        Save format ('csv', 'parquet', etc.), default is 'csv'.

    """
    downloader = DownloadOKXData(pair=pair, time_step=time_step, until=until)
    downloader.set_trades_saver(IODataBase(path, method=form))
    downloader(pair=pair)


def get_orderbook_okx(path: str, pair: str = 'BTC-USDT', time_step: int = 60,
                      until: int = 3600, form: str = 'csv') -> None:
    """ Download order book data from OKX via WebSocket.

    Parameters
    ----------
    path : str
        Path to save data.
    pair : str, optional
        Trading pair in OKX format (e.g. 'BTC-USDT'), default is 'BTC-USDT'.
    time_step : int, optional
        Seconds between snapshots, default is 60.
    until : int, optional
        Duration in seconds or stop timestamp, default is 3600.
    form : str, optional
        Save format ('csv', 'parquet', etc.), default is 'csv'.

    """
    downloader = DownloadOKXData(pair=pair, time_step=time_step, until=until)
    downloader.set_book_saver(IODataBase(path, method=form))
    downloader(pair=pair)


def get_data_okx(path: str, pair: str = 'BTC-USDT', time_step: int = 60,
                 until: int = 3600, form: str = 'csv') -> None:
    """ Download order book and trades data from OKX via WebSocket.

    Parameters
    ----------
    path : str
        Root path; trades saved under ``<path>/trades/``, book under
        ``<path>/book/``.
    pair : str, optional
        Trading pair in OKX format (e.g. 'BTC-USDT'), default is 'BTC-USDT'.
    time_step : int, optional
        Seconds between snapshots, default is 60.
    until : int, optional
        Duration in seconds or stop timestamp, default is 3600.
    form : str, optional
        Save format ('csv', 'parquet', etc.), default is 'csv'.

    """
    downloader = DownloadOKXData(pair=pair, time_step=time_step, until=until)
    downloader.set_trades_saver(IODataBase(f'{path}/trades', method=form))
    downloader.set_book_saver(IODataBase(f'{path}/book', method=form))
    downloader(pair=pair)
