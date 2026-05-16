#!/usr/bin/env python3
# coding: utf-8

from unittest.mock import MagicMock, patch

from dccd.continuous_dl.bitfinex import DownloadBitfinexData


def _make_downloader() -> DownloadBitfinexData:
    with patch.object(
        DownloadBitfinexData.__bases__[0], '__init__', return_value=None
    ):
        obj = DownloadBitfinexData.__new__(DownloadBitfinexData)
        obj._data = {}
        obj.t = 1000
        obj.d = {}
        obj.logger = MagicMock()
        obj._raw_parser = MagicMock()
        obj._parser_data = {
            'book': obj.parser_book,
            'book_raw': obj.parser_raw_book,
            'trades': obj.parser_trades,
            'trades_raw': obj.parser_raw_trades,
        }
        return obj


def test_parser_raw_book_calls_raw_parser():
    dl = _make_downloader()
    data = [0, [12345.0, 1, 0.5]]
    dl.parser_raw_book(data)
    dl._raw_parser.assert_called_once_with(
        {'price': '12345.0', 'count': 1, 'amount': 0.5}
    )


def test_parser_book_add_order():
    dl = _make_downloader()
    data = [0, [100.0, 1, 0.5]]
    dl.parser_book(data)
    assert '100.0' in dl._data[1000]['book']
    assert dl._data[1000]['book']['100.0'] == 0.5


def test_parser_book_remove_order():
    dl = _make_downloader()
    dl.d = {'100.0': {'price': '100.0', 'amount': 0.5}}
    data = [0, [100.0, 0, 0.0]]
    dl.parser_book(data)
    assert '100.0' not in dl._data[1000]['book']


def test_parser_raw_trades_skips_tu():
    dl = _make_downloader()
    data = [0, 'tu', [1, 2, 3, 4]]
    dl.parser_raw_trades(data)
    dl._raw_parser.assert_not_called()


def test_parser_raw_trades_processes_te():
    dl = _make_downloader()
    data = [0, 'te', [42, 1700000000000, 0.1, 30000.0]]
    dl.parser_raw_trades(data)
    dl._raw_parser.assert_called_once()
    parsed = dl._raw_parser.call_args[0][0]
    assert parsed['tid'] == 42
    assert parsed['type'] == 'buy'


def test_parser_trades_skips_tu():
    dl = _make_downloader()
    data = [0, 'tu', [1, 2, 3, 4]]
    dl.parser_trades(data)
    dl._raw_parser.assert_not_called()


def test_parser_trades_processes_te():
    dl = _make_downloader()
    data = [0, 'te', [42, 1700000000000, -0.1, 30000.0]]
    dl.parser_trades(data)
    dl._raw_parser.assert_called_once()
    parsed = dl._raw_parser.call_args[0][0]
    assert parsed['type'] == 'sell'
