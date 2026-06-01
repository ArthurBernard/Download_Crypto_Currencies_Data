#!/usr/bin/env python3
# coding: utf-8

""" FastAPI web UI and JSON API for the dccd daemon.

.. currentmodule:: dccd.daemon.api

This module is a thin HTTP wiring layer over the existing daemon modules
(:mod:`~dccd.daemon.config`, :mod:`~dccd.daemon.backfill`,
:mod:`~dccd.daemon.scheduler`, :mod:`~dccd.daemon.stream_manager`,
:mod:`~dccd.daemon.storage`).  No business logic lives here — every endpoint
delegates to the corresponding daemon function and returns JSON.

The web UI (Jinja2 + htmx templates under ``ui/``) consumes the same
``/api/*`` endpoints, so the front-end can be replaced without touching the
API.

Run it standalone with ``dccd ui`` or embedded in ``dccd start``.

"""

from __future__ import annotations

import collections
import json
import threading
import time
from datetime import date
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as _pkg_version
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from dccd.daemon.backfill import run_backfill
from dccd.daemon.config import CollectorConfig, load_config

__all__ = ['BackfillTracker', 'create_app']

_UI_DIR = Path(__file__).parent / 'ui'
_TEMPLATES_DIR = _UI_DIR / 'templates'
_STATIC_DIR = _UI_DIR / 'static'

_DATA_TYPES = ('ohlc', 'trades', 'orderbook')

try:
    _DCCD_VERSION = _pkg_version('dccd')
except PackageNotFoundError:  # editable install without metadata
    _DCCD_VERSION = 'dev'

# Keep at most this many finished backfill records on disk so the tracker
# file does not grow without bound; running jobs are always kept.
_MAX_FINISHED_JOBS = 50
_FINISHED_STATES = frozenset({'done', 'cancelled', 'error'})


# ---------------------------------------------------------------------------
# Backfill job tracker (disk-persisted, mirrors HealthMonitor's metrics.json)
# ---------------------------------------------------------------------------

class BackfillTracker:
    """ Track backfill jobs launched from the web UI, persisted to disk.

    State lives in ``{local_path}/.dccd/backfill_jobs.json`` so it survives
    a UI restart (the persisted ``status`` of an interrupted job stays
    ``'running'`` until overwritten — the thread itself does not survive a
    process restart).

    Parameters
    ----------
    local_path : str or Path
        Root data directory (``CollectorConfig.storage.local_path``).

    """

    def __init__(self, local_path: str | Path) -> None:
        self._dir = Path(local_path) / '.dccd'
        self._dir.mkdir(parents=True, exist_ok=True)
        self._file = self._dir / 'backfill_jobs.json'
        self._lock = threading.Lock()
        self._events: dict[str, threading.Event] = {}
        self._jobs: dict[str, dict] = self._load()

    def _load(self) -> dict[str, dict]:
        if self._file.exists():
            try:
                return json.loads(self._file.read_text())
            except Exception:
                return {}
        return {}

    def _save(self) -> None:
        self._file.write_text(json.dumps(self._jobs, indent=2))

    def _prune(self) -> None:
        """ Drop the oldest finished jobs beyond ``_MAX_FINISHED_JOBS``. """
        finished = [
            (rec.get('started_at', 0.0), jid)
            for jid, rec in self._jobs.items()
            if rec.get('status') in _FINISHED_STATES
        ]
        if len(finished) <= _MAX_FINISHED_JOBS:
            return
        finished.sort()
        for _, jid in finished[:-_MAX_FINISHED_JOBS]:
            self._jobs.pop(jid, None)

    def snapshot(self) -> dict[str, dict]:
        """ Return a snapshot of all tracked jobs. """
        with self._lock:
            return json.loads(json.dumps(self._jobs))

    def get(self, job_id: str) -> dict | None:
        """ Return a single job record, or ``None`` if unknown. """
        with self._lock:
            job = self._jobs.get(job_id)
            return dict(job) if job is not None else None

    def start(
        self,
        cfg: CollectorConfig,
        exchange: str | None,
        pairs: list[str] | None,
        start: str,
    ) -> str:
        """ Launch a backfill in a background thread and return its id. """
        slug_ex = exchange or 'all'
        slug_pairs = '-'.join(p.replace('/', '') for p in pairs) if pairs else 'all'
        job_id = f'{slug_ex}_{slug_pairs}_{int(time.time())}'

        record: dict[str, object] = {
            'id': job_id,
            'exchange': exchange,
            'pairs': pairs,
            'start': start,
            'status': 'running',
            'progress': {},
            'started_at': time.time(),
            'error': None,
        }
        with self._lock:
            self._jobs[job_id] = record
            self._prune()
            self._save()

        event = threading.Event()
        self._events[job_id] = event
        thread = threading.Thread(
            target=self._run,
            args=(job_id, cfg, exchange, pairs, start, event),
            daemon=True,
            name=f'backfill-{job_id}',
        )
        thread.start()
        return job_id

    def _run(
        self,
        job_id: str,
        cfg: CollectorConfig,
        exchange: str | None,
        pairs: list[str] | None,
        start: str,
        event: threading.Event,
    ) -> None:
        def _cb(exch: str, pair: str, done: int, total: int) -> None:
            with self._lock:
                rec = self._jobs.get(job_id)
                if rec is not None:
                    rec['progress'][pair] = {'done': done, 'total': total}
                    self._save()

        try:
            run_backfill(
                cfg, exchange=exchange, pairs=pairs, start=start,
                progress_callback=_cb, stop_event=event,
            )
            status = 'cancelled' if event.is_set() else 'done'
            with self._lock:
                self._jobs[job_id]['status'] = status
                self._save()
        except Exception as exc:  # noqa: BLE001 — surface any failure to the UI
            with self._lock:
                self._jobs[job_id]['status'] = 'error'
                self._jobs[job_id]['error'] = str(exc)
                self._save()
        finally:
            self._events.pop(job_id, None)

    def stop(self, job_id: str) -> bool:
        """ Signal a running job to cancel.  Return ``True`` if it was live. """
        event = self._events.get(job_id)
        if event is not None:
            event.set()
            with self._lock:
                if job_id in self._jobs:
                    self._jobs[job_id]['status'] = 'cancelling'
                    self._save()
            return True
        return False


# ---------------------------------------------------------------------------
# Request bodies
# ---------------------------------------------------------------------------

class _HistoJobBody(BaseModel):
    exchange: str
    pair: str
    span: int
    format: str = 'parquet'


class _BackfillBody(BaseModel):
    exchange: str | None = None
    pairs: list[str] | None = None
    start: str = '2020-01-01 00:00:00'


class _CollectBody(BaseModel):
    exchange: str | None = None
    pair: str | None = None


# ---------------------------------------------------------------------------
# Shared helpers (read fresh config per request → transparent hot-reload)
# ---------------------------------------------------------------------------

def _cfg(request: Request) -> CollectorConfig:
    return load_config(request.app.state.cfg_path)


def _check_auth(request: Request) -> None:
    """ Reject the request when a UI token is configured and not provided. """
    token = request.app.state.auth_token
    if not token:
        return
    provided: str | None = None
    auth = request.headers.get('authorization')
    if auth and auth.lower().startswith('bearer '):
        provided = auth[7:]
    if provided is None:
        provided = (
            request.cookies.get('dccd_token')
            or request.query_params.get('token')
        )
    if provided != token:
        raise HTTPException(status_code=401, detail='Invalid or missing token')


def _scan_inventory(data_path: Path) -> list[dict]:
    """ Walk *data_path* and summarise every stored Parquet series.

    Returns one dict per ``(exchange, type, pair, span)`` series with date
    range, row count, and gap count.  Mirrors the logic of the
    ``dccd inventory`` CLI command.
    """
    import polars as pl

    from dccd.tools.date_time import TS_to_date

    groups: dict[tuple[str, str, str, str], list[Path]] = collections.defaultdict(list)
    for dt in _DATA_TYPES:
        for f in data_path.glob(f'*/{dt}/**/*.parquet'):
            parts = f.relative_to(data_path).parts
            exchange_name = parts[0]
            pair_slug = parts[2]
            span_lbl = parts[3] if dt == 'ohlc' and len(parts) == 5 else '-'
            groups[(exchange_name, dt, pair_slug, span_lbl)].append(f)

    series: list[dict] = []
    for (exchange_name, dt, pair_slug, span_lbl), files in sorted(groups.items()):
        files = sorted(files)
        ts_col = pl.concat([pl.read_parquet(f, columns=['TS']) for f in files])['TS']
        total_rows = len(ts_col)
        ts_min = ts_col.min()
        ts_max = ts_col.max()
        from_date = TS_to_date(int(ts_min), form='%Y-%m-%d') if ts_min is not None else None
        to_date = TS_to_date(int(ts_max), form='%Y-%m-%d') if ts_max is not None else None

        if dt == 'ohlc':
            existing_years = {int(f.stem) for f in files}
            gaps = len(set(range(min(existing_years), max(existing_years) + 1)) - existing_years)
        else:
            existing_days = {f.stem for f in files}
            d0 = date.fromisoformat(min(existing_days))
            d1 = date.fromisoformat(max(existing_days))
            gaps = (d1 - d0).days + 1 - len(existing_days)

        series.append({
            'exchange': exchange_name,
            'pair': pair_slug.replace('-', '/', 1),
            'type': dt,
            'span': span_lbl,
            'first': from_date,
            'last': to_date,
            'rows': total_rows,
            'gaps': gaps,
        })
    return series


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def create_app(cfg_path: str | Path) -> FastAPI:
    """ Build the FastAPI application for *cfg_path*.

    Parameters
    ----------
    cfg_path : str or pathlib.Path
        Path to the YAML daemon configuration file.

    Returns
    -------
    fastapi.FastAPI
        Configured application with the JSON API (``/api/*``) and the
        htmx web UI pages mounted.

    """
    cfg_path = Path(cfg_path).expanduser().resolve()
    cfg = load_config(cfg_path)
    local_path = cfg.storage.local_path

    # When a token is configured, do not expose the unauthenticated OpenAPI
    # schema/docs — they would leak the API surface past the auth boundary.
    enable_docs = cfg.settings.ui_auth_token is None
    app = FastAPI(
        title='dccd UI',
        docs_url='/api/docs' if enable_docs else None,
        openapi_url='/openapi.json' if enable_docs else None,
    )
    app.state.cfg_path = cfg_path
    app.state.auth_token = cfg.settings.ui_auth_token
    app.state.config_lock = threading.Lock()
    app.state.tracker = BackfillTracker(local_path)
    app.state.templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))

    app.mount('/static', StaticFiles(directory=str(_STATIC_DIR)), name='static')

    _register_api(app)
    _register_pages(app)
    return app


# ---------------------------------------------------------------------------
# JSON API routes
# ---------------------------------------------------------------------------

def _register_api(app: FastAPI) -> None:
    """ Attach all ``/api/*`` JSON endpoints to *app*. """
    auth = Depends(_check_auth)

    # --- Config ---------------------------------------------------------

    @app.get('/api/config', dependencies=[auth])
    def get_config(request: Request) -> dict:
        return _cfg(request).model_dump()

    @app.put('/api/config', dependencies=[auth])
    def put_config(request: Request, body: dict) -> dict:
        try:
            validated = CollectorConfig.model_validate(body)
        except Exception as exc:
            raise HTTPException(status_code=422, detail=str(exc))
        import yaml
        normalised = validated.model_dump(mode='json')
        with request.app.state.config_lock:
            request.app.state.cfg_path.write_text(
                yaml.dump(normalised, default_flow_style=False)
            )
        return normalised

    @app.post('/api/config/validate', dependencies=[auth])
    def validate_config(body: dict) -> dict:
        try:
            CollectorConfig.model_validate(body)
        except Exception as exc:
            return {'valid': False, 'error': str(exc)}
        return {'valid': True, 'error': None}

    # --- Inventory ------------------------------------------------------

    @app.get('/api/inventory', dependencies=[auth])
    def get_inventory(request: Request) -> list[dict]:
        data_path = Path(_cfg(request).storage.local_path)
        if not data_path.exists():
            return []
        return _scan_inventory(data_path)

    # --- Metrics --------------------------------------------------------

    @app.get('/api/metrics', dependencies=[auth])
    def get_metrics(request: Request) -> dict:
        f = Path(_cfg(request).storage.local_path) / '.dccd' / 'metrics.json'
        if not f.exists():
            return {}
        try:
            return json.loads(f.read_text())
        except Exception:
            return {}

    # --- Jobs -----------------------------------------------------------

    @app.get('/api/jobs', dependencies=[auth])
    def get_jobs(request: Request) -> dict:
        cfg = _cfg(request)
        return {
            'histo_jobs': [j.model_dump() for j in cfg.histo_jobs],
            'stream_jobs': [j.model_dump() for j in cfg.stream_jobs],
        }

    @app.post('/api/jobs/histo', status_code=201, dependencies=[auth])
    def add_histo_job(request: Request, body: _HistoJobBody) -> dict:
        import yaml
        path = request.app.state.cfg_path
        with request.app.state.config_lock:
            raw: dict = yaml.safe_load(path.read_text()) or {}
            raw.setdefault('histo_jobs', [])
            raw['histo_jobs'].append({
                'exchange': body.exchange,
                'pairs': [body.pair],
                'span': body.span,
                'format': body.format,
            })
            try:
                CollectorConfig.model_validate(raw)
            except Exception as exc:
                raise HTTPException(status_code=422, detail=str(exc))
            path.write_text(yaml.dump(raw, default_flow_style=False))
        return {'status': 'added', 'exchange': body.exchange,
                'pair': body.pair, 'span': body.span}

    @app.delete('/api/jobs/histo/{exchange}/{pair_slug}/{span}', dependencies=[auth])
    def remove_histo_job(
        request: Request, exchange: str, pair_slug: str, span: int,
    ) -> dict:
        import yaml
        pair = pair_slug.replace('-', '/', 1)
        path = request.app.state.cfg_path
        with request.app.state.config_lock:
            raw: dict = yaml.safe_load(path.read_text()) or {}
            jobs: list = raw.get('histo_jobs', [])
            target = next(
                (j for j in jobs if j.get('exchange') == exchange
                 and j.get('span') == span and pair in j.get('pairs', [])),
                None,
            )
            if target is None:
                raise HTTPException(
                    status_code=404,
                    detail=f'No job: {exchange} {pair} {span}s',
                )
            target['pairs'].remove(pair)
            if not target['pairs']:
                jobs.remove(target)
            try:
                CollectorConfig.model_validate(raw)
            except Exception as exc:
                raise HTTPException(status_code=422, detail=str(exc))
            path.write_text(yaml.dump(raw, default_flow_style=False))
        return {'status': 'removed', 'exchange': exchange,
                'pair': pair, 'span': span}

    # --- Backfill -------------------------------------------------------

    @app.post('/api/backfill', status_code=202, dependencies=[auth])
    def start_backfill(request: Request, body: _BackfillBody) -> dict:
        cfg = _cfg(request)
        job_id = request.app.state.tracker.start(
            cfg, body.exchange, body.pairs, body.start,
        )
        return {'id': job_id}

    @app.get('/api/backfill', dependencies=[auth])
    def list_backfill(request: Request) -> dict:
        return request.app.state.tracker.snapshot()

    @app.get('/api/backfill/{job_id}', dependencies=[auth])
    def get_backfill(request: Request, job_id: str) -> dict:
        job = request.app.state.tracker.get(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail=f'No backfill {job_id}')
        return job

    @app.delete('/api/backfill/{job_id}', dependencies=[auth])
    def cancel_backfill(request: Request, job_id: str) -> dict:
        tracker = request.app.state.tracker
        if tracker.get(job_id) is None:
            raise HTTPException(status_code=404, detail=f'No backfill {job_id}')
        live = tracker.stop(job_id)
        return {'status': 'cancelling' if live else 'not_running'}

    # --- Collect --------------------------------------------------------

    @app.post('/api/collect', status_code=202, dependencies=[auth])
    def start_collect(request: Request, body: _CollectBody | None = None) -> dict:
        from dccd.daemon.health import HealthMonitor
        from dccd.daemon.scheduler import run_histo_job, run_once
        cfg = _cfg(request)
        health = HealthMonitor(cfg.storage.local_path, cfg.alerts)
        exchange = body.exchange if body else None
        pair = body.pair if body else None

        if exchange is None and pair is None:
            def _job() -> None:
                run_once(cfg, health=health)
        else:
            targets = [
                (job, p)
                for job in cfg.histo_jobs
                if exchange is None or job.exchange == exchange
                for p in job.pairs
                if pair is None or p == pair
            ]
            if not targets:
                raise HTTPException(status_code=404, detail='No matching histo job')

            def _job() -> None:
                for job, p in targets:
                    run_histo_job(job, p, cfg.storage.local_path,
                                  cfg.settings.timezone, health)

        threading.Thread(target=_job, daemon=True, name='ui-collect').start()
        return {'status': 'started'}

    # --- Logs -----------------------------------------------------------

    @app.get('/api/logs', dependencies=[auth])
    def get_logs(request: Request, tail: int = 200) -> dict:
        f = Path(_cfg(request).storage.local_path) / '.dccd' / 'dccd.log'
        if not f.exists():
            return {'lines': []}
        lines = f.read_text(errors='replace').splitlines()
        return {'lines': lines[-tail:]}

    # --- Storage --------------------------------------------------------

    @app.get('/api/storage', dependencies=[auth])
    def get_storage(request: Request) -> dict:
        from dccd.daemon.storage import RemoteStorage
        cfg = _cfg(request)
        storage = RemoteStorage(cfg.storage)
        last_sync = None
        f = Path(cfg.storage.local_path) / '.dccd' / 'last_sync.json'
        if f.exists():
            try:
                last_sync = json.loads(f.read_text())
            except Exception:
                last_sync = None
        return {
            'remotes': [r.model_dump() for r in cfg.storage.remotes],
            'sync_interval': cfg.storage.sync_interval,
            'rclone_available': storage.check_rclone(),
            'last_sync': last_sync,
        }

    @app.post('/api/storage/sync', status_code=202, dependencies=[auth])
    def trigger_sync(request: Request) -> dict:
        from dccd.daemon.stream_manager import SyncService
        cfg = _cfg(request)
        if not cfg.storage.remotes:
            raise HTTPException(status_code=400, detail='No remotes configured')
        svc = SyncService(cfg.storage)
        threading.Thread(target=svc.sync_now, daemon=True, name='ui-sync').start()
        return {'status': 'started'}


# ---------------------------------------------------------------------------
# HTML page routes (htmx shells consuming the JSON API)
# ---------------------------------------------------------------------------

def _register_pages(app: FastAPI) -> None:
    """ Attach the HTML page routes (served by Jinja2 templates). """
    auth = Depends(_check_auth)

    def _page(name: str, request: Request) -> HTMLResponse:
        resp = request.app.state.templates.TemplateResponse(
            request, name,
            {'request': request, 'version': _DCCD_VERSION,
             'active': request.url.path},
        )
        token = request.query_params.get('token')
        if token and token == request.app.state.auth_token:
            resp.set_cookie('dccd_token', token, httponly=True, samesite='strict')
        return resp

    @app.get('/', response_class=HTMLResponse, dependencies=[auth])
    def page_dashboard(request: Request) -> HTMLResponse:
        return _page('dashboard.html', request)

    @app.get('/inventory', response_class=HTMLResponse, dependencies=[auth])
    def page_inventory(request: Request) -> HTMLResponse:
        return _page('inventory.html', request)

    @app.get('/jobs', response_class=HTMLResponse, dependencies=[auth])
    def page_jobs(request: Request) -> HTMLResponse:
        return _page('jobs.html', request)

    @app.get('/logs', response_class=HTMLResponse, dependencies=[auth])
    def page_logs(request: Request) -> HTMLResponse:
        return _page('logs.html', request)

    @app.get('/config', response_class=HTMLResponse, dependencies=[auth])
    def page_config(request: Request) -> HTMLResponse:
        return _page('config.html', request)

    @app.get('/storage', response_class=HTMLResponse, dependencies=[auth])
    def page_storage(request: Request) -> HTMLResponse:
        return _page('storage.html', request)
