#!/usr/bin/env python3
# coding: utf-8

import time

import pytest

from dccd import FromKraken as fk

OHLC_KEYS = ['date', 'open', 'high', 'low', 'close', 'volume', 'quoteVolume']


@pytest.fixture
def loader(tmp_data_path):
    return fk(tmp_data_path, 'XBT', 86400, 'USD')


def test_import_data(loader, mock_kraken):
    start = int(time.time() // 86400 * 86400 - 86400)
    data = loader._import_data(start=start)
    assert isinstance(data, list)
    assert len(data) > 0
    assert isinstance(data[0], dict)
    for key in OHLC_KEYS:
        assert key in data[0]
