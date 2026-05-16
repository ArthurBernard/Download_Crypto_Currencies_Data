#!/usr/bin/env python3
# coding: utf-8

from unittest.mock import MagicMock, patch

import pytest

from dccd.continuous_dl.kraken import (
    DownloadKrakenData,
    _parser_book,
    _parser_kline,
    _parser_trades,
)

# =========================================================================== #
#                           Module-level parsers                              #
# =========================================================================== #

_TRADE_DATA = [
    {'trade_id': 1, 'price': 30000.0, 'qty': 0.5, 'side': 'buy',
     'timestamp': '2023-11-14T22:13:20.000000Z', 'ord_type': 'market'},
    {'trade_id': 2, 'price': 29999.0, 'qty': 1.0, 'side': 'sell',
     'timestamp': '2023-11-14T22:13:21.000000Z', 'ord_type': 'limit'},
]

_BOOK_DATA = [
    {
        'symbol': 'BTC/USD',
        'bids': [{'price': 29990.0, 'qty': 2.0}, {'price': 29980.0, 'qty': 0.0}],
        'asks': [{'price': 30010.0, 'qty': 1.5}],
        'checksum': 12345,
        'timestamp': '2023-11-14T22:13:20.000000Z',
    }
]

_KLINE_DATA = [
    {
        'symbol': 'BTC/USD',
        'open': 30000.0, 'high': 30100.0, 'low': 29900.0, 'close': 30050.0,
        'volume': 10.5, 'vwap': 30020.0, 'trades': 50,
        'interval_begin': '2023-11-14T22:00:00.000000Z',
        'interval': 60,
        'timestamp': '2023-11-14T22:01:00.000000Z',
    }
]


def test_parser_trades_buy():
    result = _parser_trades(_TRADE_DATA)
    assert len(result) == 2
    assert result[0]['type'] == 'buy'
    assert result[0]['price'] == 30000.0


def test_parser_trades_sell():
    result = _parser_trades(_TRADE_DATA)
    assert result[1]['type'] == 'sell'


def test_parser_book_bids_and_asks():
    result = _parser_book(_BOOK_DATA)
    assert result['29990.0'] == 2.0
    assert '-30010.0' in result
    assert result['-30010.0'] == -1.5


def test_parser_book_zero_qty_included():
    result = _parser_book(_BOOK_DATA)
    assert result['29980.0'] == 0.0


def test_parser_kline():
    result = _parser_kline(_KLINE_DATA)
    assert len(result) == 1
    assert result[0]['open'] == 30000.0
    assert result[0]['volume'] == 10.5


# =========================================================================== #
#                         DownloadKrakenData tests                            #
# =========================================================================== #


def _make_downloader() -> DownloadKrakenData:
    with patch.object(
        DownloadKrakenData.__bases__[0], '__init__', return_value=None
    ):
        obj = DownloadKrakenData.__new__(DownloadKrakenData)
        obj._data = {}
        obj.t = 2000
        obj.d = {}
        obj.logger = MagicMock()
        return obj


def test_parser_trades_appends_to_data():
    dl = _make_downloader()
    dl.parser_trades(_TRADE_DATA)
    assert 2000 in dl._data
    assert len(dl._data[2000]['trades']) == 2
    assert dl._data[2000]['trades'][0]['price'] == 30000.0


def test_parser_book_updates_and_removes():
    dl = _make_downloader()
    msg = {'channel': 'book', 'type': 'snapshot', 'data': _BOOK_DATA}
    dl.parser_book(msg)
    book = dl._data[2000]['book']
    assert '29990.0' in book
    assert book['29990.0'] == 2.0
    assert '29980.0' not in book


def test_parser_kline_appends():
    dl = _make_downloader()
    dl.parser_kline(_KLINE_DATA)
    assert 2000 in dl._data
    assert dl._data[2000]['trades'][0]['open'] == 30000.0


@pytest.mark.asyncio
async def test_on_message_trade():
    dl = _make_downloader()
    dl.parser_trades = MagicMock()
    msg = {'channel': 'trade', 'type': 'update', 'data': _TRADE_DATA}
    await dl.on_message(msg)
    dl.parser_trades.assert_called_once_with(_TRADE_DATA)


@pytest.mark.asyncio
async def test_on_message_book():
    dl = _make_downloader()
    dl.parser_book = MagicMock()
    msg = {'channel': 'book', 'type': 'snapshot', 'data': _BOOK_DATA}
    await dl.on_message(msg)
    dl.parser_book.assert_called_once_with(msg)


@pytest.mark.asyncio
async def test_on_message_ohlc():
    dl = _make_downloader()
    dl.parser_kline = MagicMock()
    msg = {'channel': 'ohlc', 'type': 'update', 'data': _KLINE_DATA}
    await dl.on_message(msg)
    dl.parser_kline.assert_called_once_with(_KLINE_DATA)


@pytest.mark.asyncio
async def test_on_message_heartbeat_ignored():
    dl = _make_downloader()
    dl.parser_trades = MagicMock()
    dl.parser_book = MagicMock()
    await dl.on_message({'channel': 'heartbeat'})
    dl.parser_trades.assert_not_called()
    dl.parser_book.assert_not_called()


@pytest.mark.asyncio
async def test_on_message_unknown_ignored():
    dl = _make_downloader()
    dl.parser_trades = MagicMock()
    await dl.on_message({'channel': 'ticker', 'data': []})
    dl.parser_trades.assert_not_called()
