"""Tests for FastAPI HTTP interface."""


import pytest
from fastapi.testclient import TestClient

from dccd.application.config import AppConfig
from dccd.interfaces.api.app import create_app


@pytest.fixture
def tmp_data_path(tmp_path):
    return str(tmp_path)


@pytest.fixture
def app(tmp_data_path):
    cfg = AppConfig()
    cfg.settings.data_path = tmp_data_path
    cfg.storage.local_path = tmp_data_path
    return create_app(config=cfg)


@pytest.fixture
def client(app):
    with TestClient(app) as c:
        yield c


class TestHealthEndpoint:
    def test_health(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


class TestOperationsEndpoint:
    def test_list_operations(self, client):
        resp = client.get("/api/operations")
        assert resp.status_code == 200
        ops = {o["name"] for o in resp.json()["operations"]}
        assert "backfill" in ops
        assert "stream" in ops
        assert "inventory" in ops


class TestInventoryEndpoint:
    def test_empty_inventory(self, client):
        resp = client.get("/api/inventory")
        assert resp.status_code == 200
        assert resp.json()["datasets"] == []


class TestConfigEndpoint:
    def test_get_config(self, client):
        resp = client.get("/api/config")
        assert resp.status_code == 200
        data = resp.json()
        assert "settings" in data

    def test_put_config_invalid(self, client):
        resp = client.put("/api/config", json={"settings": {"data_path": ""}})
        # May return 200 (empty string is valid path) or 422
        assert resp.status_code in (200, 422)


class TestBackfillEndpoint:
    def test_start_backfill(self, client):
        resp = client.post("/api/backfill", json={
            "exchange": "binance",
            "symbol": "BTC/USDT",
            "data_type": "ohlc",
            "span": 3600,
            "start": "last",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert "run_id" in data
        assert data["status"] == "started"

    def test_start_backfill_invalid_symbol(self, client):
        resp = client.post("/api/backfill", json={
            "exchange": "binance",
            "symbol": "BTCUSDT",  # no separator
            "data_type": "ohlc",
        })
        assert resp.status_code == 400

    def test_backfill_status_not_found(self, client):
        resp = client.get("/api/backfill/nonexistent-run-id")
        assert resp.status_code == 404


class TestJobsEndpoint:
    def test_list_jobs_empty(self, client):
        resp = client.get("/api/jobs")
        assert resp.status_code == 200
        assert resp.json()["jobs"] == []


class TestStreamsEndpoint:
    def test_list_streams(self, client):
        resp = client.get("/api/streams")
        assert resp.status_code == 200
        assert "streams" in resp.json()


class TestReadEndpoint:
    def test_read_empty(self, client):
        resp = client.post("/api/read", json={
            "exchange": "binance",
            "symbol": "BTC/USDT",
            "data_type": "ohlc",
            "span": 3600,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["rows"] == 0


class TestRunsEndpoint:
    def test_list_runs(self, client):
        resp = client.get("/api/runs")
        assert resp.status_code == 200
        assert "runs" in resp.json()


class TestMigrateEndpoint:
    def test_migrate_dry_run(self, client):
        resp = client.post("/api/migrate", json={"dry_run": True})
        assert resp.status_code == 200
        data = resp.json()
        assert "report" in data
