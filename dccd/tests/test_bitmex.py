#!/usr/bin/env python3
# coding: utf-8

from unittest.mock import MagicMock, patch

import pytest

from dccd.continuous_dl.bitmex import DownloadBitmexData


def _make_downloader() -> DownloadBitmexData:
    with patch.object(
        DownloadBitmexData.__bases__[0], '__init__', return_value=None
    ):
        obj = DownloadBitmexData.__new__(DownloadBitmexData)
        obj._data = {}
        obj.t = 1000
        obj.d = {}
        obj.start = False
        obj.logger = MagicMock()
        obj.parser = MagicMock()
        return obj


_PARTIAL_MSG = {
    'action': 'partial',
    'data': [{'id': 1, 'side': 'Buy', 'size': 10, 'price': 30000.0}],
}

_DELETE_MSG = {
    'action': 'delete',
    'data': [{'id': 1, 'side': 'Buy', 'size': 10}],
}

_UPDATE_MSG = {
    'action': 'update',
    'data': [{'id': 1, 'side': 'Buy', 'size': 20}],
}

_TRADE_MSG = {
    'data': [
        {
            'timestamp': '2023-11-01T00:00:00.000Z',
            'price': 30000.0,
            'size': 1,
            'side': 'Buy',
        },
        {
            'timestamp': '2023-11-01T00:00:00.001Z',
            'price': 29999.0,
            'size': 2,
            'side': 'Sell',
        },
    ]
}


def test_parser_book_partial_populates():
    dl = _make_downloader()
    dl.parser_book(_PARTIAL_MSG)
    assert dl.start is True
    assert 1 in dl.d
    assert dl._data[1000][30000.0] == 10


def test_parser_book_delete_removes_entry():
    dl = _make_downloader()
    dl.parser_book(_PARTIAL_MSG)
    dl.parser_book(_DELETE_MSG)
    assert 1 not in dl.d
    assert 30000.0 not in dl._data[1000]


def test_parser_book_update_changes_amount():
    dl = _make_downloader()
    dl.parser_book(_PARTIAL_MSG)
    dl.parser_book(_UPDATE_MSG)
    assert dl._data[1000][30000.0] == 20


def test_parser_trades_aggregates_in_same_timestep():
    dl = _make_downloader()
    dl.parser_trades(_TRADE_MSG)
    assert len(dl._data[1000]) == 2
    dl.parser_trades(_TRADE_MSG)
    assert len(dl._data[1000]) == 4


@pytest.mark.asyncio
async def test_on_message_no_action_logs_info():
    dl = _make_downloader()
    await dl.on_message({'subscribe': 'ok'})
    dl.logger.info.assert_called_once()
    dl.parser.assert_not_called()


@pytest.mark.asyncio
async def test_on_message_with_action_calls_parser():
    dl = _make_downloader()
    await dl.on_message(_PARTIAL_MSG)
    dl.parser.assert_called_once_with(_PARTIAL_MSG)


@pytest.mark.asyncio
async def test_on_message_non_dict_logs_error():
    dl = _make_downloader()
    await dl.on_message([1, 2, 3])
    dl.logger.error.assert_called_once()
