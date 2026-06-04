#!/usr/bin/env python3
# coding: utf-8

import time

import pytest

from dccd import FromCoinbase as fc
from dccd.histo_dl.coinbase import FromCoinbase

OHLC_KEYS = ['date', 'open', 'high', 'low', 'close', 'volume', 'quoteVolume']
TRADE_KEYS = ['tid', 'timestamp', 'price', 'amount', 'type']


@pytest.mark.parametrize('crypto,fiat,expected', [
    ('BTC', 'USD',  'BTC-USD'),
    ('ETH', 'EUR',  'ETH-EUR'),
    ('XBT', 'USD',  'BTC-USD'),  # XBT alias → BTC
])
def test_format_pair(crypto, fiat, expected):
    assert FromCoinbase.format_pair(crypto, fiat) == expected


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


def test_import_data_chunks_large_window(tmp_data_path, monkeypatch):
    # Coinbase caps ~300 candles/request; a window wider than 300*span must be
    # paged into multiple requests instead of one oversized (HTTP 400) call.
    from unittest.mock import MagicMock

    loader = fc(tmp_data_path, 'XBT', 60, 'USD')
    calls = []

    def fake_get(url, params=None, *a, **kw):
        calls.append(params)
        m = MagicMock()
        m.status_code = 200
        m.json.return_value = [[params['start'] and 1_700_000_000, 1, 2, 0, 1, 5]]
        return m

    monkeypatch.setattr('requests.get', fake_get)
    end = 1_700_040_000
    start = end - 40_000  # > 300*60 = 18000 → at least 3 chunks
    data = loader._import_data(start=start, end=end)
    assert len(calls) >= 3
    assert len(data) == len(calls)


def test_http_500_raises(loader, mock_http_500):
    with pytest.raises(ValueError):
        loader._import_data(start=0)


def test_malformed_response_raises(loader, monkeypatch):
    from unittest.mock import MagicMock
    m = MagicMock()
    m.status_code = 200
    m.json.return_value = {"error": "bad"}
    monkeypatch.setattr("requests.get", lambda *a, **kw: m)
    with pytest.raises((TypeError, ValueError, RuntimeError)):
        loader._import_data(start=0)


def test_import_trades(loader, mock_coinbase_trades):
    data = loader._import_trades(start=0, end=int(time.time()))
    assert isinstance(data, list)
    assert len(data) > 0
    for key in TRADE_KEYS:
        assert key in data[0]


def test_import_orderbook(loader, mock_coinbase_book):
    data = loader._import_orderbook(depth=2)
    assert isinstance(data, list)
    assert len(data) > 0
    sides = {d['side'] for d in data}
    assert 'bid' in sides
    assert 'ask' in sides


def test_import_trades_http_500_raises(loader, mock_http_500):
    with pytest.raises(ValueError):
        loader._import_trades(start=0, end=1)
