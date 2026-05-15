#!/usr/bin/env python3
# coding: utf-8

from unittest.mock import MagicMock, patch

import pytest

from dccd.continuous_dl.okx import (
    DownloadOKXData,
    _parser_book,
    _parser_kline,
    _parser_trades,
)

# =========================================================================== #
#                           Module-level parsers                              #
# =========================================================================== #

_TRADE_DATA = [
    {'tradeId': '130639474', 'px': '42219.9', 'sz': '0.001', 'side': 'buy',
     'ts': '1630048573000'},
    {'tradeId': '130639475', 'px': '42100.0', 'sz': '0.5', 'side': 'sell',
     'ts': '1630048574000'},
]

_BOOK_DATA = [
    {
        'bids': [['41006.8', '0.6', '0', '1'], ['41000.0', '1.0', '0', '1']],
        'asks': [['41010.0', '0.3', '0', '1']],
        'ts': '1629966436396',
    }
]

_KLINE_DATA = [
    ['1630048500000', '42000.0', '42300.0', '41900.0', '42200.0', '10.5',
     '441100', '441100', '1'],
]


def test_parser_trades_buy():
    result = _parser_trades(_TRADE_DATA)
    assert len(result) == 2
    assert result[0]['type'] == 'buy'
    assert result[0]['price'] == 42219.9
    assert result[0]['timestamp'] == 1630048573.0


def test_parser_trades_sell():
    result = _parser_trades(_TRADE_DATA)
    assert result[1]['type'] == 'sell'


def test_parser_book_bids_and_asks():
    result = _parser_book(_BOOK_DATA)
    assert result['41006.8'] == 0.6
    assert '-41010.0' in result
    assert result['-41010.0'] == -0.3


def test_parser_book_zero_qty_included():
    data = [{'bids': [['41000.0', '0']], 'asks': []}]
    result = _parser_book(data)
    assert result['41000.0'] == 0.0


def test_parser_kline():
    result = _parser_kline(_KLINE_DATA)
    assert len(result) == 1
    assert result[0]['open'] == 42000.0
    assert result[0]['close'] == 42200.0
    assert result[0]['timestamp'] == 1630048500.0


# =========================================================================== #
#                           DownloadOKXData tests                             #
# =========================================================================== #


def _make_downloader() -> DownloadOKXData:
    with patch.object(
        DownloadOKXData.__bases__[0], '__init__', return_value=None
    ):
        obj = DownloadOKXData.__new__(DownloadOKXData)
        obj._data = {}
        obj.t = 2000
        obj.d = {}
        obj.logger = MagicMock()
        return obj


def test_parser_trades_appends_to_data():
    dl = _make_downloader()
    dl.parser_trades(_TRADE_DATA)
    assert 2000 in dl._data
    assert len(dl._data[2000]) == 2
    assert dl._data[2000][0]['price'] == 42219.9


def test_parser_book_updates_and_removes():
    dl = _make_downloader()
    msg = {'arg': {'channel': 'books50-l2-tbt'}, 'action': 'snapshot',
           'data': _BOOK_DATA}
    dl.parser_book(msg)
    assert '41006.8' in dl._data[2000]
    # zero-qty bid should be removed
    assert '0' not in dl._data[2000]


def test_parser_kline_appends():
    dl = _make_downloader()
    dl.parser_kline(_KLINE_DATA)
    assert 2000 in dl._data
    assert dl._data[2000][0]['open'] == 42000.0


@pytest.mark.asyncio
async def test_on_message_trades():
    dl = _make_downloader()
    dl.parser_trades = MagicMock()
    msg = {'arg': {'channel': 'trades', 'instId': 'BTC-USDT'}, 'data': _TRADE_DATA}
    await dl.on_message(msg)
    dl.parser_trades.assert_called_once_with(_TRADE_DATA)


@pytest.mark.asyncio
async def test_on_message_book():
    dl = _make_downloader()
    dl.parser_book = MagicMock()
    msg = {'arg': {'channel': 'books50-l2-tbt', 'instId': 'BTC-USDT'},
           'action': 'snapshot', 'data': _BOOK_DATA}
    await dl.on_message(msg)
    dl.parser_book.assert_called_once_with(msg)


@pytest.mark.asyncio
async def test_on_message_kline():
    dl = _make_downloader()
    dl.parser_kline = MagicMock()
    msg = {'arg': {'channel': 'candle1H', 'instId': 'BTC-USDT'}, 'data': _KLINE_DATA}
    await dl.on_message(msg)
    dl.parser_kline.assert_called_once_with(_KLINE_DATA)


@pytest.mark.asyncio
async def test_on_message_unknown_does_nothing():
    dl = _make_downloader()
    dl.parser_trades = MagicMock()
    dl.parser_book = MagicMock()
    await dl.on_message({'arg': {'channel': 'tickers'}, 'data': []})
    dl.parser_trades.assert_not_called()
    dl.parser_book.assert_not_called()
