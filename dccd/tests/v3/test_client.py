"""Smoke tests for the public async Client facade (WS-E / E1)."""

import pytest

from dccd import Client
from dccd.application.config import SUPPORTED_EXCHANGES


@pytest.mark.asyncio
async def test_client_wires_all_exchanges(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "noconfig"))
    async with Client() as c:
        for ex in SUPPORTED_EXCHANGES:
            assert c._registry.get(ex) is not None


@pytest.mark.asyncio
async def test_client_read_empty(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path / "noconfig"))
    async with Client() as c:
        c._store = c._store.__class__(tmp_path)  # point at an empty dir
        df = c.read("binance", "BTC/USDT", "ohlc", span=3600)
        assert df.is_empty()
        assert c.inventory() == []
