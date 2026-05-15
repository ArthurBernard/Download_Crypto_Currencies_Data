#!/usr/bin/env python3
# coding: utf-8

import time

import pytest

from dccd import FromCoinbase as fc

OHLC_KEYS = ['date', 'open', 'high', 'low', 'close', 'volume', 'quoteVolume']


@pytest.fixture
def loader(tmp_data_path):
    return fc(tmp_data_path, 'XBT', 86400, 'USD')


def test_import_data(loader, mock_coinbase):
    start = int(time.time() // 86400 * 86400 - 86400)
    data = loader._import_data(start=start)
    assert isinstance(data, list)
    assert len(data) > 0
    assert isinstance(data[0], dict)
    for key in OHLC_KEYS:
        assert key in data[0]


def test_http_500_raises(loader, mock_http_500):
    with pytest.raises(ValueError):
        loader._import_data(start=0)


def test_malformed_response_raises(loader, monkeypatch):
    from unittest.mock import MagicMock
    m = MagicMock()
    m.status_code = 200
    m.json.return_value = {"error": "bad"}
    monkeypatch.setattr("requests.get", lambda *a, **kw: m)
    with pytest.raises((TypeError, ValueError)):
        loader._import_data(start=0)
