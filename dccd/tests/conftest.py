#!/usr/bin/env python3
# coding: utf-8

import json
from unittest.mock import MagicMock

import pytest

_TS = 1746057600  # 2025-05-01 00:00:00 UTC (daily candle)


def _mock_response(payload):
    m = MagicMock()
    m.text = json.dumps(payload)
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
