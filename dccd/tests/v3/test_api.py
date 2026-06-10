"""Tests for FastAPI HTTP interface."""


import pytest
from fastapi.testclient import TestClient

from dccd.application.config import AppConfig, RemoteConfig
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


class TestBackfillCancel:
    def test_cancel_unknown_run_404(self, client):
        assert client.delete("/api/backfill/nope").status_code == 404


class TestAuth:
    @pytest.fixture
    def auth_client(self, tmp_data_path):
        cfg = AppConfig()
        cfg.settings.data_path = tmp_data_path
        cfg.storage.local_path = tmp_data_path
        cfg.settings.ui_auth_token = "s3cret"
        with TestClient(create_app(config=cfg)) as c:
            yield c

    def test_api_requires_token(self, auth_client):
        assert auth_client.get("/api/inventory").status_code == 401

    def test_api_accepts_bearer(self, auth_client):
        r = auth_client.get("/api/inventory", headers={"Authorization": "Bearer s3cret"})
        assert r.status_code == 200

    def test_api_rejects_wrong_token(self, auth_client):
        r = auth_client.get("/api/inventory", headers={"Authorization": "Bearer nope"})
        assert r.status_code == 401

    def test_health_stays_open(self, auth_client):
        assert auth_client.get("/health").status_code == 200

    def test_no_token_means_open(self, client):
        assert client.get("/api/inventory").status_code == 200

    def test_api_accepts_query_token(self, auth_client):
        # SSE/EventSource path: ?token= still authorises (non-browser clients).
        r = auth_client.get("/api/inventory?token=s3cret")
        assert r.status_code == 200


class TestAuthSession:
    """Browser login/session: page routes gated, token never templated."""

    @pytest.fixture
    def cfg(self, tmp_data_path):
        cfg = AppConfig()
        cfg.settings.data_path = tmp_data_path
        cfg.storage.local_path = tmp_data_path
        cfg.settings.ui_auth_token = "s3cret"
        return cfg

    @pytest.fixture
    def auth_client(self, cfg):
        with TestClient(create_app(config=cfg)) as c:
            yield c

    def test_page_redirects_to_login_when_unauthenticated(self, auth_client):
        r = auth_client.get("/", follow_redirects=False)
        assert r.status_code == 303
        assert r.headers["location"] == "/login?next=/"

    def test_login_page_renders(self, auth_client):
        r = auth_client.get("/login")
        assert r.status_code == 200
        assert "token" in r.text.lower()

    def test_login_wrong_token_401(self, auth_client):
        r = auth_client.post(
            "/login", data={"token": "nope", "next": "/"}, follow_redirects=False
        )
        assert r.status_code == 401

    def test_login_sets_httponly_lax_cookie_and_grants_access(self, cfg):
        with TestClient(create_app(config=cfg)) as c:
            r = c.post(
                "/login", data={"token": "s3cret", "next": "/"}, follow_redirects=False
            )
            assert r.status_code == 303
            set_cookie = r.headers["set-cookie"].lower()
            assert "dccd_session=" in set_cookie
            assert "httponly" in set_cookie
            assert "samesite=lax" in set_cookie
            assert "samesite=none" not in set_cookie  # CSRF: never cross-site
            # The session cookie now drives both pages and the API.
            assert c.get("/", follow_redirects=False).status_code == 200
            assert c.get("/api/inventory").status_code == 200

    def test_api_rejects_without_cookie_or_bearer(self, auth_client):
        # Fresh client (no cookie) cannot reach the API.
        assert auth_client.get("/api/inventory").status_code == 401

    def test_logout_clears_session(self, cfg):
        with TestClient(create_app(config=cfg)) as c:
            c.post("/login", data={"token": "s3cret", "next": "/"})
            assert c.get("/", follow_redirects=False).status_code == 200
            c.post("/logout", follow_redirects=False)
            assert c.get("/", follow_redirects=False).status_code == 303

    def test_token_never_in_page(self, cfg):
        with TestClient(create_app(config=cfg)) as c:
            c.post("/login", data={"token": "s3cret", "next": "/"})
            body = c.get("/").text
            assert "s3cret" not in body  # regression: token must not leak into HTML

    def test_open_redirect_blocked(self, cfg):
        for bad in ("//evil.com", "https://evil", "/\\evil"):
            with TestClient(create_app(config=cfg)) as c:
                r = c.post(
                    "/login", data={"token": "s3cret", "next": bad},
                    follow_redirects=False,
                )
                assert r.headers["location"] == "/", bad
        with TestClient(create_app(config=cfg)) as c:
            r = c.post(
                "/login", data={"token": "s3cret", "next": "/data"},
                follow_redirects=False,
            )
            assert r.headers["location"] == "/data"

    def test_no_token_pages_open(self, client):
        # Default localhost (no token): pages render without a login.
        assert client.get("/", follow_redirects=False).status_code == 200


class _OkRemote:
    """In-test remote that reports success without invoking rclone."""

    async def sync_all(self):
        return {"r:bucket": True}


class TestRemoteSync:
    @pytest.fixture
    def synced_app(self, tmp_data_path):
        cfg = AppConfig()
        cfg.settings.data_path = tmp_data_path
        cfg.storage.local_path = tmp_data_path
        cfg.storage.remotes = [RemoteConfig(remote="r:bucket")]
        cfg.storage.sync_interval = 1800
        return create_app(config=cfg)

    def test_get_sync_not_configured(self, client):
        body = client.get("/api/storage/sync").json()
        assert body["configured"] is False
        assert body["remotes"] == []
        assert body["last"] is None

    def test_get_sync_configured_with_last(self, synced_app, tmp_data_path):
        from dccd.application.service_factory import build_runs_store

        with TestClient(synced_app) as c:
            runs = build_runs_store(tmp_data_path)
            runs.create_run("remote-sync@1", "remote-sync", "sync", "-", "all", "-")
            runs.finish_run("remote-sync@1", "succeeded", rows_written=1)
            body = c.get("/api/storage/sync").json()
            assert body["configured"] is True
            assert body["remotes"] == ["r:bucket"]
            assert body["sync_interval"] == 1800
            assert body["last"]["state"] == "succeeded"
            assert body["next_eta"] is not None

    def test_post_sync_no_remotes_400(self, client):
        assert client.post("/api/storage/sync").status_code == 400

    def test_post_sync_started(self, synced_app):
        with TestClient(synced_app) as c:
            c.app.state.remote = _OkRemote()  # avoid spawning a real rclone call
            r = c.post("/api/storage/sync")
            assert r.status_code == 200
            assert r.json()["status"] == "started"


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

    def test_run_job_not_found(self, client):
        resp = client.post("/api/jobs/run", json={"job_id": "nonexistent:job:id"})
        assert resp.status_code == 404

    def test_run_all_jobs_empty(self, client):
        resp = client.post("/api/jobs/run-all")
        assert resp.status_code == 200
        assert resp.json()["started"] == 0


class TestJobCrudEndpoints:
    def test_create_then_list_then_delete(self, client):
        r = client.post("/api/jobs/create", json={
            "operation": "backfill", "exchange": "binance",
            "symbol": "BTC/USDT", "data_type": "ohlc", "span": 3600,
            "start": "2024-01-01",
        })
        assert r.status_code == 200, r.text
        job_id = r.json()["job_id"]

        jobs = client.get("/api/jobs").json()["jobs"]
        created = next((j for j in jobs if j["id"] == job_id), None)
        assert created is not None
        # the listing must expose start/snapshot_interval/depth so the UI can
        # render and preserve them (a missing start blanked the date field).
        assert created["start"] == "2024-01-01"
        assert "snapshot_interval" in created and "depth" in created

        # duplicate is rejected
        dup = client.post("/api/jobs/create", json={
            "operation": "backfill", "exchange": "binance",
            "symbol": "BTC/USDT", "data_type": "ohlc", "span": 3600,
        })
        assert dup.status_code == 400

        d = client.post("/api/jobs/delete", json={"job_id": job_id})
        assert d.status_code == 200
        jobs = client.get("/api/jobs").json()["jobs"]
        assert not any(j["id"] == job_id for j in jobs)

    def test_delete_unknown_404(self, client):
        assert client.post("/api/jobs/delete", json={"job_id": "nope"}).status_code == 404

    def test_update_start(self, client):
        job_id = client.post("/api/jobs/create", json={
            "operation": "backfill", "exchange": "kraken",
            "symbol": "ETH/USD", "data_type": "ohlc", "span": 86400,
        }).json()["job_id"]
        u = client.post("/api/jobs/update", json={"job_id": job_id, "start": "2020-06-01"})
        assert u.status_code == 200
        assert client.post("/api/jobs/update",
                           json={"job_id": "nope", "start": "2020-01-01"}).status_code == 404

    def test_update_schedule(self, client):
        job_id = client.post("/api/jobs/create", json={
            "operation": "backfill", "exchange": "kraken",
            "symbol": "ETH/USD", "data_type": "ohlc", "span": 3600,
            "trigger_kind": "manual",
        }).json()["job_id"]
        # Set a daily cron.
        u = client.post("/api/jobs/update",
                        json={"job_id": job_id, "schedule": True, "every": 86400})
        assert u.status_code == 200
        job = next(j for j in client.get("/api/jobs").json()["jobs"] if j["id"] == job_id)
        assert job["trigger"] == "interval" and job["every"] == 86400
        # Interval below the span is rejected.
        bad = client.post("/api/jobs/update",
                          json={"job_id": job_id, "schedule": True, "every": 60})
        assert bad.status_code == 400
        # Clearing returns to manual.
        client.post("/api/jobs/update",
                    json={"job_id": job_id, "schedule": True, "every": None})
        job = next(j for j in client.get("/api/jobs").json()["jobs"] if j["id"] == job_id)
        assert job["trigger"] == "manual"

    def test_create_stream_appears_in_streams(self, client):
        client.post("/api/jobs/create", json={
            "operation": "stream", "exchange": "binance",
            "symbol": "BTC/USDT", "data_type": "trades",
            "trigger_kind": "supervised",
        })
        streams = client.get("/api/streams").json()["streams"]
        assert any("binance" in s["id"] and "trades" in s["id"] for s in streams)

    def test_delete_stream_unregisters_worker(self, client):
        # Deleting a stream job must also drop its worker from /api/streams,
        # otherwise a deleted stream keeps running and stays controllable.
        jid = client.post("/api/jobs/create", json={
            "operation": "stream", "exchange": "binance",
            "symbol": "BTC/USDT", "data_type": "trades",
            "trigger_kind": "supervised",
        }).json()["job_id"]
        assert any(s["id"] == jid for s in client.get("/api/streams").json()["streams"])
        client.post("/api/jobs/delete", json={"job_id": jid})
        assert not any(s["id"] == jid for s in client.get("/api/streams").json()["streams"])


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
    def test_migrate_endpoint_removed(self, client):
        # The v2→v3 migrate feature has been removed entirely.
        resp = client.post("/api/migrate", json={"dry_run": True})
        assert resp.status_code == 404
