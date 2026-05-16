#!/usr/bin/env python3
# coding: utf-8

import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dccd.continuous_dl.binance import DownloadBinanceData, _parser_book, _parser_trades

# =========================================================================== #
#                           Module-level parsers                              #
# =========================================================================== #

_TRADE_DATA = {
    't': 1001,
    'T': 1700000000000,
    'p': '30000.0',
    'q': '0.5',
    'm': False,  # buyer is taker → buy
}

_TRADE_DATA_SELL = {
    't': 1002,
    'T': 1700000001000,
    'p': '29999.0',
    'q': '1.0',
    'm': True,  # buyer is maker → sell
}

_BOOK_DATA = {
    'b': [['29990.0', '2.0'], ['29980.0', '0']],
    'a': [['30010.0', '1.5']],
}


def test_parser_trades_buy():
    result = _parser_trades(_TRADE_DATA)
    assert len(result) == 1
    assert result[0]['type'] == 'buy'
    assert result[0]['price'] == 30000.0
    assert result[0]['timestamp'] == 1700000000.0


def test_parser_trades_sell():
    result = _parser_trades(_TRADE_DATA_SELL)
    assert result[0]['type'] == 'sell'
    assert result[0]['price'] == 29999.0


def test_parser_book_bids_and_asks():
    result = _parser_book(_BOOK_DATA)
    assert result['29990.0'] == 2.0
    assert '-30010.0' in result
    assert result['-30010.0'] == -1.5


def test_parser_book_zero_qty_included():
    result = _parser_book(_BOOK_DATA)
    assert result['29980.0'] == 0.0


# =========================================================================== #
#                         DownloadBinanceData tests                           #
# =========================================================================== #


def _make_downloader() -> DownloadBinanceData:
    with patch.object(
        DownloadBinanceData.__bases__[0], '__init__', return_value=None
    ):
        obj = DownloadBinanceData.__new__(DownloadBinanceData)
        obj._data = {}
        obj.t = 2000
        obj.d = {}
        obj.logger = MagicMock()
        return obj


def test_parser_trades_appends_to_data():
    dl = _make_downloader()
    dl.parser_trades(_TRADE_DATA)
    assert 2000 in dl._data
    assert len(dl._data[2000]['trades']) == 1
    assert dl._data[2000]['trades'][0]['price'] == 30000.0


def test_parser_book_updates_and_removes():
    dl = _make_downloader()
    dl.parser_book(_BOOK_DATA)
    book = dl._data[2000]['book']
    assert '29990.0' in book
    assert book['29990.0'] == 2.0
    assert '29980.0' not in book


def test_parser_book_does_not_overwrite_trades():
    dl = _make_downloader()
    dl.parser_trades(_TRADE_DATA)
    dl.parser_book(_BOOK_DATA)
    assert len(dl._data[2000]['trades']) == 1
    assert '29990.0' in dl._data[2000]['book']


@pytest.mark.asyncio
async def test_on_message_trade():
    dl = _make_downloader()
    dl.parser_trades = MagicMock()
    msg = {'stream': 'btcusdt@trade', 'data': _TRADE_DATA}
    await dl.on_message(msg)
    dl.parser_trades.assert_called_once_with(_TRADE_DATA)


@pytest.mark.asyncio
async def test_on_message_book():
    dl = _make_downloader()
    dl.parser_book = MagicMock()
    msg = {'stream': 'btcusdt@depth50@100ms', 'data': _BOOK_DATA}
    await dl.on_message(msg)
    dl.parser_book.assert_called_once_with(_BOOK_DATA)


@pytest.mark.asyncio
async def test_on_message_unknown_does_nothing():
    dl = _make_downloader()
    dl.parser_trades = MagicMock()
    dl.parser_book = MagicMock()
    await dl.on_message({'stream': 'btcusdt@miniTicker', 'data': {}})
    dl.parser_trades.assert_not_called()
    dl.parser_book.assert_not_called()


# =========================================================================== #
#                        snapshot_ts and checkpoint tests                     #
# =========================================================================== #


@pytest.mark.asyncio
async def test_anext_adds_snapshot_ts():
    dl = _make_downloader()
    dl.ts = 60
    dl.until = time.time() + 3600
    dl._data[dl.t] = {'trades': [{'price': 1.0}], 'book': {}}
    before = int(time.time() * 1000)
    payload = await dl.__anext__()
    after = int(time.time() * 1000)
    assert payload is not None
    assert before <= payload['snapshot_ts'] <= after


def test_checkpoint_save_and_load(tmp_path: Path):
    dl = _make_downloader()
    dl._checkpoint_dir = tmp_path
    dl.pair = 'BTCUSDT'
    dl.d = {'30000.0': 1.5, '-30010.0': -0.5}

    dl._save_checkpoint()

    dl2 = _make_downloader()
    dl2._checkpoint_dir = tmp_path
    dl2.pair = 'BTCUSDT'
    dl2._load_checkpoint()

    assert dl2.d == {'30000.0': 1.5, '-30010.0': -0.5}


def test_checkpoint_no_dir_does_nothing(tmp_path: Path):
    dl = _make_downloader()
    dl._checkpoint_dir = None
    dl.d = {'30000.0': 1.0}
    dl._save_checkpoint()
    assert list(tmp_path.iterdir()) == []


def test_set_trades_saver_stored():
    dl = _make_downloader()
    saver = MagicMock()
    dl.set_trades_saver(saver)
    assert dl._trades_saver is saver


def test_set_book_saver_stored():
    dl = _make_downloader()
    saver = MagicMock()
    dl.set_book_saver(saver)
    assert dl._book_saver is saver
