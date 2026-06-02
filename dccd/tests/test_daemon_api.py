#!/usr/bin/env python3
# coding: utf-8

"""Tests for the dccd web UI / JSON API (dccd.daemon.api)."""

from __future__ import annotations

import json
import time

import polars as pl
import pytest
import yaml
from fastapi.testclient import TestClient

from dccd.daemon.api import create_app
from dccd.storage import DataStore

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_BASE_CONFIG = {
    'settings': {'data_path': None},  # filled in per-fixture
    'histo_jobs': [{'exchange': 'binance', 'pairs': ['BTC/USDT'], 'span': 3600}],
}


def _write_config(tmp_path, extra=None):
    cfg = dict(_BASE_CONFIG)
    cfg['settings'] = {'data_path': str(tmp_path / 'data')}
    if extra:
        cfg.update(extra)
    path = tmp_path / 'config.yml'
    path.write_text(yaml.dump(cfg))
    return path


@pytest.fixture
def client(tmp_path):
    path = _write_config(tmp_path)
    return TestClient(create_app(path))


@pytest.fixture
def cfg_path(tmp_path):
    return _write_config(tmp_path)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def test_get_config(client):
    r = client.get('/api/config')
    assert r.status_code == 200
    body = r.json()
    assert body['histo_jobs'][0]['exchange'] == 'binance'
    assert body['settings']['ui_port'] == 8080


def test_put_config_valid_rewrites_yaml(tmp_path, cfg_path):
    client = TestClient(create_app(cfg_path))
    new = client.get('/api/config').json()
    new['settings']['timezone'] = 'UTC'
    r = client.put('/api/config', json=new)
    assert r.status_code == 200
    on_disk = yaml.safe_load(cfg_path.read_text())
    assert on_disk['settings']['timezone'] == 'UTC'


def test_put_config_invalid_returns_422(client):
    bad = {'settings': {}, 'histo_jobs': []}  # no jobs → validation error
    r = client.put('/api/config', json=bad)
    assert r.status_code == 422


def test_validate_config_endpoint(client):
    good = client.get('/api/config').json()
    assert client.post('/api/config/validate', json=good).json()['valid'] is True
    bad = {'histo_jobs': [], 'stream_jobs': []}
    out = client.post('/api/config/validate', json=bad).json()
    assert out['valid'] is False
    assert out['error']


# ---------------------------------------------------------------------------
# Inventory
# ---------------------------------------------------------------------------

def test_inventory_empty(client):
    assert client.get('/api/inventory').json() == []


def test_inventory_reports_date_range(tmp_path):
    path = _write_config(tmp_path)
    data_path = tmp_path / 'data'
    store = DataStore(str(data_path), 'binance', 'BTC/USDT', 3600, 'ohlc')
    ts = [1577836800, 1577840400]  # 2020-01-01 00:00 and 01:00 UTC
    store.save(pl.DataFrame({
        'TS': ts, 'open': [1.0, 1.0], 'high': [1.0, 1.0],
        'low': [1.0, 1.0], 'close': [1.0, 1.0], 'volume': [1.0, 1.0],
    }))
    client = TestClient(create_app(path))
    rows = client.get('/api/inventory').json()
    assert len(rows) == 1
    s = rows[0]
    assert s['exchange'] == 'binance'
    assert s['pair'] == 'BTC/USDT'
    assert s['type'] == 'ohlc'
    assert s['first'] == '2020-01-01'
    assert s['rows'] == 2


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

def test_metrics_absent(client):
    assert client.get('/api/metrics').json() == {}


def test_metrics_populated(tmp_path):
    path = _write_config(tmp_path)
    dccd_dir = tmp_path / 'data' / '.dccd'
    dccd_dir.mkdir(parents=True)
    (dccd_dir / 'metrics.json').write_text(json.dumps({
        'binance/BTC/USDT': {
            'last_run_at': 1.0, 'last_success_at': 1.0,
            'rows_collected': 120, 'errors_count': 0,
        }
    }))
    client = TestClient(create_app(path))
    m = client.get('/api/metrics').json()
    assert m['binance/BTC/USDT']['rows_collected'] == 120


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------

def test_jobs_list(client):
    data = client.get('/api/jobs').json()
    assert data['histo_jobs'][0]['pairs'] == ['BTC/USDT']
    assert data['stream_jobs'] == []


# ---------------------------------------------------------------------------
# Backfill (mock run_backfill so no network calls happen)
# ---------------------------------------------------------------------------

def test_backfill_lifecycle(tmp_path, monkeypatch):
    import dccd.daemon.api as api_mod

    started = {}

    def fake_run_backfill(cfg, exchange=None, pairs=None, start='', **kw):
        started['exchange'] = exchange
        cb = kw.get('progress_callback')
        if cb and pairs:
            cb(exchange, pairs[0], 1, 2)

    monkeypatch.setattr(api_mod, 'run_backfill', fake_run_backfill)
    client = TestClient(create_app(_write_config(tmp_path)))

    r = client.post('/api/backfill',
                    json={'exchange': 'binance', 'pairs': ['BTC/USDT']})
    assert r.status_code == 202
    job_id = r.json()['id']

    listed = client.get('/api/backfill').json()
    assert job_id in listed

    got = client.get(f'/api/backfill/{job_id}')
    assert got.status_code == 200
    assert started['exchange'] == 'binance'


def test_backfill_forwards_parallel(tmp_path, monkeypatch):
    import dccd.daemon.api as api_mod

    seen = {}

    def fake_run_backfill(cfg, exchange=None, pairs=None, start='', **kw):
        seen['parallel'] = kw.get('parallel')

    monkeypatch.setattr(api_mod, 'run_backfill', fake_run_backfill)
    client = TestClient(create_app(_write_config(tmp_path)))

    r = client.post('/api/backfill',
                    json={'exchange': 'binance', 'pairs': ['BTC/USDT'],
                          'parallel': True})
    assert r.status_code == 202
    for _ in range(50):
        if 'parallel' in seen:
            break
        time.sleep(0.02)
    assert seen['parallel'] is True


def test_backfill_unknown_404(client):
    assert client.get('/api/backfill/nope').status_code == 404
    assert client.delete('/api/backfill/nope').status_code == 404


def test_backfill_no_matching_job_400(client):
    # kraken has no configured histo job → reject instead of a silent no-op.
    r = client.post('/api/backfill', json={'exchange': 'kraken'})
    assert r.status_code == 400
    assert 'histo job' in r.json()['detail']


# ---------------------------------------------------------------------------
# Collect
# ---------------------------------------------------------------------------

class _SyncThread:
    """ Drop-in for threading.Thread that runs the target synchronously. """

    def __init__(self, target=None, daemon=None, name=None, args=(), kwargs=None):
        self._target = target
        self._args = args
        self._kwargs = kwargs or {}

    def start(self):
        if self._target:
            self._target(*self._args, **self._kwargs)


def test_collect_all_runs_run_once(tmp_path, monkeypatch):
    import dccd.daemon.api as api_mod
    import dccd.daemon.scheduler as sched

    called = {}
    monkeypatch.setattr(sched, 'run_once',
                        lambda cfg, health=None: called.setdefault('once', True))
    monkeypatch.setattr(api_mod.threading, 'Thread', _SyncThread)
    client = TestClient(create_app(_write_config(tmp_path)))
    r = client.post('/api/collect')
    assert r.status_code == 202
    assert called.get('once') is True


def test_collect_filtered_runs_matching_jobs(tmp_path, monkeypatch):
    import dccd.daemon.api as api_mod
    import dccd.daemon.scheduler as sched

    runs = []
    monkeypatch.setattr(
        sched, 'run_histo_job',
        lambda job, pair, base_path, tz, health: runs.append((job.exchange, pair)))
    monkeypatch.setattr(api_mod.threading, 'Thread', _SyncThread)
    client = TestClient(create_app(_write_config(tmp_path)))
    r = client.post('/api/collect', json={'exchange': 'binance', 'pair': 'BTC/USDT'})
    assert r.status_code == 202
    assert runs == [('binance', 'BTC/USDT')]


def test_collect_unknown_returns_404(client):
    assert client.post('/api/collect', json={'exchange': 'kraken'}).status_code == 404


# ---------------------------------------------------------------------------
# Streams
# ---------------------------------------------------------------------------

_STREAM_EXTRA = {
    'stream_jobs': [{
        'exchange': 'binance', 'pairs': ['BTC/USDT'],
        'channels': ['trades'], 'time_step': 60,
    }],
}


def test_streams_list_reflects_config(tmp_path):
    client = TestClient(create_app(_write_config(tmp_path, extra=_STREAM_EXTRA)))
    rows = client.get('/api/streams').json()
    assert len(rows) == 1
    assert rows[0]['exchange'] == 'binance'
    assert rows[0]['pair'] == 'BTC/USDT'
    assert rows[0]['channels'] == ['trades']
    assert rows[0]['running'] is False


def test_streams_start_calls_manager(tmp_path):
    app = create_app(_write_config(tmp_path, extra=_STREAM_EXTRA))
    started = {}
    app.state.stream_manager.start_one = (
        lambda job, pair, channels: started.setdefault(
            'key', f'{job.exchange}_{pair}_{channels}') or 'k'
    )
    client = TestClient(app)
    r = client.post('/api/streams/start',
                    json={'exchange': 'binance', 'pair': 'BTC/USDT', 'channels': ['trades']})
    assert r.status_code == 202
    assert started['key'] == "binance_BTC/USDT_['trades']"


def test_streams_start_unknown_job_404(tmp_path):
    client = TestClient(create_app(_write_config(tmp_path, extra=_STREAM_EXTRA)))
    r = client.post('/api/streams/start',
                    json={'exchange': 'kraken', 'pair': 'BTC/USD', 'channels': ['trades']})
    assert r.status_code == 404


def test_streams_stop_not_running(tmp_path):
    client = TestClient(create_app(_write_config(tmp_path, extra=_STREAM_EXTRA)))
    r = client.post('/api/streams/stop',
                    json={'exchange': 'binance', 'pair': 'BTC/USDT', 'channels': ['trades']})
    assert r.status_code == 200
    assert r.json()['status'] == 'not_running'


def test_create_app_uses_injected_stream_manager(tmp_path):
    from dccd.daemon.config import load_config
    from dccd.daemon.stream_manager import StreamManager

    path = _write_config(tmp_path, extra=_STREAM_EXTRA)
    sm = StreamManager(load_config(path))
    app = create_app(path, sm)
    assert app.state.stream_manager is sm


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------

def test_scheduler_status_starts_stopped(client):
    body = client.get('/api/scheduler').json()
    assert body['running'] is False
    assert len(body['jobs']) == 1  # one (exchange, pair) histo job


def test_scheduler_start_stop_toggle(client):
    assert client.post('/api/scheduler/start').json()['running'] is True
    assert client.get('/api/scheduler').json()['running'] is True
    assert client.post('/api/scheduler/stop').json()['running'] is False
    assert client.get('/api/scheduler').json()['running'] is False
    # Resuming a paused scheduler works too.
    assert client.post('/api/scheduler/start').json()['running'] is True


def test_create_app_uses_injected_scheduler_and_health(tmp_path):
    from dccd.daemon.config import load_config
    from dccd.daemon.health import HealthMonitor
    from dccd.daemon.scheduler import build_histo_scheduler

    path = _write_config(tmp_path)
    cfg = load_config(path)
    health = HealthMonitor(cfg.storage.local_path, cfg.alerts)
    scheduler = build_histo_scheduler(cfg, health)
    app = create_app(path, health=health, scheduler=scheduler)
    assert app.state.health is health
    assert app.state.scheduler is scheduler


def test_create_app_wires_logging(tmp_path):
    # Constructing the app must create the log file via HealthMonitor.
    path = _write_config(tmp_path)
    create_app(path)
    assert (tmp_path / 'data' / '.dccd' / 'dccd.log').exists()


# ---------------------------------------------------------------------------
# Logs
# ---------------------------------------------------------------------------

def test_logs_tail(tmp_path):
    path = _write_config(tmp_path)
    dccd_dir = tmp_path / 'data' / '.dccd'
    dccd_dir.mkdir(parents=True)
    (dccd_dir / 'dccd.log').write_text('\n'.join(f'line {i}' for i in range(20)))
    client = TestClient(create_app(path))
    lines = client.get('/api/logs?tail=5').json()['lines']
    assert lines == [f'line {i}' for i in range(15, 20)]


def test_logs_absent(client):
    assert client.get('/api/logs').json() == {'lines': []}


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

def test_storage_no_remotes(client):
    s = client.get('/api/storage').json()
    assert s['remotes'] == []
    assert s['rclone_available'] is False
    assert s['last_sync'] is None


def test_storage_sync_without_remotes_400(client):
    assert client.post('/api/storage/sync').status_code == 400


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def test_auth_required_when_token_set(tmp_path):
    path = _write_config(tmp_path, extra={
        'settings': {'data_path': str(tmp_path / 'data'),
                     'ui_auth_token': 'secret'},
    })
    client = TestClient(create_app(path))
    assert client.get('/api/config').status_code == 401
    ok = client.get('/api/config', headers={'Authorization': 'Bearer secret'})
    assert ok.status_code == 200
    assert client.get('/api/config',
                      headers={'Authorization': 'Bearer wrong'}).status_code == 401


def test_openapi_disabled_when_token_set(tmp_path):
    path = _write_config(tmp_path, extra={
        'settings': {'data_path': str(tmp_path / 'data'),
                     'ui_auth_token': 'secret'},
    })
    client = TestClient(create_app(path))
    assert client.get('/openapi.json').status_code == 404
    assert client.get('/api/docs').status_code == 404


def test_openapi_enabled_without_token(client):
    assert client.get('/openapi.json').status_code == 200


# ---------------------------------------------------------------------------
# HTML pages
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('path', [
    '/', '/inventory', '/jobs', '/logs', '/config', '/storage',
])
def test_pages_render(client, path):
    r = client.get(path)
    assert r.status_code == 200
    assert 'text/html' in r.headers['content-type']
