#!/usr/bin/env python3
# coding: utf-8

import time

import pytest

from dccd import FromBybit

OHLC_KEYS = ['date', 'open', 'high', 'low', 'close', 'volume', 'quoteVolume']


@pytest.fixture
def loader(tmp_data_path):
    return FromBybit(tmp_data_path, 'BTC', 86400)


def test_import_data(loader, mock_bybit):
    start = int(time.time() // 86400 * 86400 - 86400)
    data = loader._import_data(start=start)
    assert isinstance(data, list)
    assert len(data) > 0
    assert isinstance(data[0], dict)
    for key in OHLC_KEYS:
        assert key in data[0]


def test_pair_format(loader):
    assert loader.pair == 'BTCUSDT'
