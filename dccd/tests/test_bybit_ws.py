#!/usr/bin/env python3
# coding: utf-8

from unittest.mock import MagicMock, patch

import pytest

from dccd.continuous_dl.bybit import DownloadBybitData, _parser_book, _parser_trades

# =========================================================================== #
#                           Module-level parsers                              #
# =========================================================================== #

_TRADE_MSG = {
    'data': [
        {'i': '1001', 'T': 1700000000000, 'p': '30000.0', 'v': '0.5', 'S': 'Buy'},
        {'i': '1002', 'T': 1700000001000, 'p': '29999.0', 'v': '1.0', 'S': 'Sell'},
    ]
}

_BOOK_MSG = {
    'data': {
        'b': [['29990.0', '2.0'], ['29980.0', '0']],
        'a': [['30010.0', '1.5']],
    }
}


def test_parser_trades_buy():
    result = _parser_trades(_TRADE_MSG)
    assert len(result) == 2
    assert result[0]['type'] == 'buy'
    assert result[0]['price'] == 30000.0


def test_parser_trades_sell():
    result = _parser_trades(_TRADE_MSG)
    assert result[1]['type'] == 'sell'


def test_parser_book_bids_and_asks():
    result = _parser_book(_BOOK_MSG)
    assert result['29990.0'] == 2.0
    assert '-30010.0' in result
    assert result['-30010.0'] == -1.5


def test_parser_book_zero_qty_included():
    result = _parser_book(_BOOK_MSG)
    assert result['29980.0'] == 0.0


# =========================================================================== #
#                          DownloadBybitData tests                            #
# =========================================================================== #


def _make_downloader() -> DownloadBybitData:
    with patch.object(
        DownloadBybitData.__bases__[0], '__init__', return_value=None
    ):
        obj = DownloadBybitData.__new__(DownloadBybitData)
        obj._data = {}
        obj.t = 2000
        obj.d = {}
        obj.logger = MagicMock()
        return obj


def test_parser_trades_appends_to_data():
    dl = _make_downloader()
    dl.parser_trades(_TRADE_MSG)
    assert 2000 in dl._data
    assert len(dl._data[2000]) == 2
    assert dl._data[2000][0]['price'] == 30000.0


def test_parser_book_updates_and_removes():
    dl = _make_downloader()
    dl.parser_book(_BOOK_MSG)
    # qty > 0 → kept
    assert '29990.0' in dl._data[2000]
    assert dl._data[2000]['29990.0'] == 2.0
    # qty == 0 → removed from book
    assert '29980.0' not in dl._data[2000]


@pytest.mark.asyncio
async def test_on_message_trade_topic():
    dl = _make_downloader()
    dl.parser_trades = MagicMock()
    msg = {'topic': 'publicTrade.BTCUSDT', **_TRADE_MSG}
    await dl.on_message(msg)
    dl.parser_trades.assert_called_once_with(msg)


@pytest.mark.asyncio
async def test_on_message_orderbook_topic():
    dl = _make_downloader()
    dl.parser_book = MagicMock()
    msg = {'topic': 'orderbook.50.BTCUSDT', **_BOOK_MSG}
    await dl.on_message(msg)
    dl.parser_book.assert_called_once_with(msg)


@pytest.mark.asyncio
async def test_on_message_unknown_topic_does_nothing():
    dl = _make_downloader()
    dl.parser_trades = MagicMock()
    dl.parser_book = MagicMock()
    await dl.on_message({'topic': 'unknown', 'data': []})
    dl.parser_trades.assert_not_called()
    dl.parser_book.assert_not_called()
