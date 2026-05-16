#!/usr/bin/env python3
# coding: utf-8

import time

import pytest

from dccd import FromBinance as fb
from dccd.histo_dl.binance import FromBinance

OHLC_KEYS = ['date', 'open', 'high', 'low', 'close', 'volume', 'quoteVolume']


@pytest.mark.parametrize('crypto,fiat,expected', [
    ('BTC',  'USDT', 'BTCUSDT'),
    ('ETH',  'USDT', 'ETHUSDT'),
    ('XBT',  'USDT', 'BTCUSDT'),  # XBT alias → BTC
])
def test_format_pair(crypto, fiat, expected):
    assert FromBinance.format_pair(crypto, fiat) == expected


@pytest.fixture
def loader(tmp_data_path):
    return fb(tmp_data_path, 'XBT', 86400, 'USD')


def test_import_data(loader, mock_binance):
    start = int(time.time() // 86400 * 86400 - 86400)
    data = loader._import_data(start=start)
    assert isinstance(data, list)
    assert len(data) > 0
    assert isinstance(data[0], dict)
    for key in OHLC_KEYS:
        assert key in data[0]


def test_rate_limit_retry(loader, mock_429_then_200):
    start = int(time.time() // 86400 * 86400 - 86400)
    data = loader._import_data(start=start)
    assert len(data) > 0
    assert len(mock_429_then_200) == 3


def test_http_500_raises(loader, mock_http_500):
    with pytest.raises(ValueError):
        loader._import_data(start=0)


def test_malformed_response_raises(loader, monkeypatch):
    monkeypatch.setattr("requests.get", lambda *a, **kw: _mock_bad())
    with pytest.raises((KeyError, TypeError, ValueError)):
        loader._import_data(start=0)


def _mock_bad():
    from unittest.mock import MagicMock
    m = MagicMock()
    m.status_code = 200
    m.json.return_value = {"error": "bad"}
    return m
