#!/usr/bin/env python3
# coding: utf-8

import json
from unittest.mock import MagicMock

import pytest

_TS = 1746057600  # 2025-05-01 00:00:00 UTC (daily candle)


def _mock_response(payload):
    m = MagicMock()
    m.text = json.dumps(payload)
    m.status_code = 200
    m.json.return_value = payload
    return m


@pytest.fixture
def tmp_data_path(tmp_path):
    return str(tmp_path)


@pytest.fixture
def mock_binance(monkeypatch):
    payload = [[
        _TS * 1000, "50000", "51000", "49000", "50500", "100",
        (_TS + 86399) * 1000, "5050000", 1000, "50", "2525000", "0"
    ]]
    monkeypatch.setattr("requests.get", lambda *a, **kw: _mock_response(payload))


@pytest.fixture
def mock_coinbase(monkeypatch):
    payload = [[_TS, 49000, 51000, 50000, 50500, 100.0]]
    monkeypatch.setattr("requests.get", lambda *a, **kw: _mock_response(payload))


@pytest.fixture
def mock_kraken(monkeypatch):
    payload = {
        "error": [],
        "result": {
            "XXBTZUSD": [[_TS, "50000", "51000", "49000", "50500", "50200", "100", 1000]],
            "last": _TS,
        },
    }
    monkeypatch.setattr("requests.get", lambda *a, **kw: _mock_response(payload))


@pytest.fixture
def mock_bybit(monkeypatch):
    payload = {
        "result": {
            "list": [
                [str(_TS * 1000), "50000", "51000", "49000", "50500", "100", "5050000"],
            ]
        }
    }
    monkeypatch.setattr("requests.get", lambda *a, **kw: _mock_response(payload))


@pytest.fixture
def mock_okx(monkeypatch):
    payload = {
        "data": [
            [str(_TS * 1000), "50000", "51000", "49000", "50500", "100", "100", "5050000", "1"],
        ]
    }
    monkeypatch.setattr("requests.get", lambda *a, **kw: _mock_response(payload))


# ---------------------------------------------------------------------------
# Trades mocks
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_binance_trades(monkeypatch):
    payload = [
        {'a': 1, 'T': _TS * 1000, 'p': '50000', 'q': '0.1', 'm': False},
        {'a': 2, 'T': (_TS + 1) * 1000, 'p': '50100', 'q': '0.2', 'm': True},
    ]
    monkeypatch.setattr("requests.get", lambda *a, **kw: _mock_response(payload))


@pytest.fixture
def mock_kraken_trades(monkeypatch):
    payload = {
        'result': {
            'XXBTZUSD': [
                ['50000', '0.1', float(_TS), 'b', 'l', '', 1],
                ['50100', '0.2', float(_TS + 1), 's', 'l', '', 2],
            ],
            'last': str(_TS + 1),
        }
    }
    monkeypatch.setattr("requests.get", lambda *a, **kw: _mock_response(payload))


@pytest.fixture
def mock_bybit_trades(monkeypatch):
    payload = {
        'result': {
            'list': [
                {'time': str(_TS * 1000), 'price': '50000', 'size': '0.1', 'side': 'Buy'},
                {'time': str((_TS + 1) * 1000), 'price': '50100', 'size': '0.2', 'side': 'Sell'},
            ]
        }
    }
    monkeypatch.setattr("requests.get", lambda *a, **kw: _mock_response(payload))


@pytest.fixture
def mock_okx_trades(monkeypatch):
    payload = {
        'data': [
            {'tradeId': '1001', 'ts': str(_TS * 1000), 'px': '50000', 'sz': '0.1', 'side': 'buy'},
            {'tradeId': '1002', 'ts': str((_TS + 1) * 1000), 'px': '50100', 'sz': '0.2', 'side': 'sell'},
        ]
    }
    monkeypatch.setattr("requests.get", lambda *a, **kw: _mock_response(payload))


@pytest.fixture
def mock_coinbase_trades(monkeypatch):
    payload = [
        {'trade_id': 1, 'time': '2025-05-01T00:00:00Z', 'price': '50000', 'size': '0.1', 'side': 'buy'},
        {'trade_id': 2, 'time': '2025-05-01T00:00:01Z', 'price': '50100', 'size': '0.2', 'side': 'sell'},
    ]
    monkeypatch.setattr("requests.get", lambda *a, **kw: _mock_response(payload))


# ---------------------------------------------------------------------------
# Order book mocks
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_binance_depth(monkeypatch):
    payload = {
        'bids': [['50000', '1.0'], ['49900', '2.0']],
        'asks': [['50100', '0.5'], ['50200', '1.5']],
    }
    monkeypatch.setattr("requests.get", lambda *a, **kw: _mock_response(payload))


@pytest.fixture
def mock_kraken_depth(monkeypatch):
    payload = {
        'result': {
            'XXBTZUSD': {
                'bids': [['50000', '1.0', _TS], ['49900', '2.0', _TS]],
                'asks': [['50100', '0.5', _TS], ['50200', '1.5', _TS]],
            }
        }
    }
    monkeypatch.setattr("requests.get", lambda *a, **kw: _mock_response(payload))


@pytest.fixture
def mock_bybit_orderbook(monkeypatch):
    payload = {
        'result': {
            'b': [['50000', '1.0'], ['49900', '2.0']],
            'a': [['50100', '0.5'], ['50200', '1.5']],
        }
    }
    monkeypatch.setattr("requests.get", lambda *a, **kw: _mock_response(payload))


@pytest.fixture
def mock_okx_books(monkeypatch):
    payload = {
        'data': [{
            'bids': [['50000', '1.0', '0', '2'], ['49900', '2.0', '0', '3']],
            'asks': [['50100', '0.5', '0', '1'], ['50200', '1.5', '0', '4']],
            'ts': str(_TS * 1000),
        }]
    }
    monkeypatch.setattr("requests.get", lambda *a, **kw: _mock_response(payload))


@pytest.fixture
def mock_coinbase_book(monkeypatch):
    payload = {
        'bids': [['50000', '1.0', 2], ['49900', '2.0', 1]],
        'asks': [['50100', '0.5', 1], ['50200', '1.5', 3]],
    }
    monkeypatch.setattr("requests.get", lambda *a, **kw: _mock_response(payload))


@pytest.fixture
def mock_http_500(monkeypatch):
    """Simulate an HTTP 500 response whose body is not valid JSON."""
    m = MagicMock()
    m.status_code = 500
    m.json.side_effect = ValueError("Server error — no JSON body")
    monkeypatch.setattr("requests.get", lambda *a, **kw: m)


@pytest.fixture
def mock_429_then_200(monkeypatch):
    """Simulate two 429 responses then a 200."""
    calls = []
    payload = [[
        _TS * 1000, "50000", "51000", "49000", "50500", "100",
        (_TS + 86399) * 1000, "5050000", 1000, "50", "2525000", "0"
    ]]

    def _side_effect(*a, **kw):
        calls.append(1)
        if len(calls) < 3:
            r = MagicMock()
            r.status_code = 429
            r.raise_for_status.side_effect = __import__('requests').HTTPError(
                response=r
            )
            return r
        return _mock_response(payload)

    monkeypatch.setattr("requests.get", _side_effect)
    return calls
