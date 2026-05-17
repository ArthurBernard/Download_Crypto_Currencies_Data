#!/usr/bin/env python3
# coding: utf-8

import time

import pytest

from dccd import FromOKX
from dccd.histo_dl.okx import FromOKX as _FromOKX

OHLC_KEYS = ['date', 'open', 'high', 'low', 'close', 'volume', 'quoteVolume']
TRADE_KEYS = ['tid', 'timestamp', 'price', 'amount', 'type']


@pytest.mark.parametrize('crypto,fiat,expected', [
    ('BTC', 'USDT', 'BTC-USDT'),
    ('ETH', 'USDT', 'ETH-USDT'),
    ('SOL', 'BTC',  'SOL-BTC'),
])
def test_format_pair(crypto, fiat, expected):
    assert _FromOKX.format_pair(crypto, fiat) == expected


@pytest.fixture
def loader(tmp_data_path):
    return FromOKX(tmp_data_path, 'BTC', 86400)


def test_import_data(loader, mock_okx):
    start = int(time.time() // 86400 * 86400 - 86400)
    data = loader._import_data(start=start)
    assert isinstance(data, list)
    assert len(data) > 0
    assert isinstance(data[0], dict)
    for key in OHLC_KEYS:
        assert key in data[0]


def test_pair_format(loader):
    assert loader.pair == 'BTC-USDT'


def test_http_500_raises(loader, mock_http_500):
    with pytest.raises(ValueError):
        loader._import_data(start=0)


def test_malformed_response_raises(loader, monkeypatch):
    from unittest.mock import MagicMock
    m = MagicMock()
    m.status_code = 200
    m.json.return_value = {}
    monkeypatch.setattr("requests.get", lambda *a, **kw: m)
    with pytest.raises(KeyError):
        loader._import_data(start=0)


def test_import_trades(loader, mock_okx_trades):
    data = loader._import_trades(start=0, end=int(time.time()))
    assert isinstance(data, list)
    assert len(data) > 0
    for key in TRADE_KEYS:
        assert key in data[0]


def test_import_orderbook(loader, mock_okx_books):
    data = loader._import_orderbook(depth=2)
    assert isinstance(data, list)
    assert len(data) > 0
    sides = {d['side'] for d in data}
    assert 'bid' in sides
    assert 'ask' in sides


def test_import_trades_http_500_raises(loader, mock_http_500):
    with pytest.raises(ValueError):
        loader._import_trades(start=0, end=1)
