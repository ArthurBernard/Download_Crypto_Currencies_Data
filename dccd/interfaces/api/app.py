"""FastAPI application — routes 1:1 with the Operation Registry + SSE events.

The UI (``/``, ``/inventory``, …) is a thin client that consumes this API;
all business logic lives in ``dccd.application``.

Architecture note
-----------------
All mutable server state lives in ``app.state`` and is initialised in the
``lifespan`` context manager. Endpoint helpers (``_store``, ``_bus``, …) read
from ``app.state`` via the ``Request`` object so the wiring is always explicit
and testable.

Background-task safety
----------------------
``start_backfill`` spawns an ``asyncio.Task``. The task captures *references*
to the infrastructure objects (``reg``, ``store``, ``runs_store``, ``bus``)
as local variables **before** the task is created — not via the ``Request``
object, which Starlette may recycle after the response is sent.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import pathlib
from collections.abc import Coroutine
from typing import Any, cast

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from dccd.application.config import AppConfig, load_config, resolve_config_path
from dccd.application.events import EventBus
from dccd.application.jobs import JobParams, JobSpec, JobTarget, Trigger
from dccd.application.registry import REGISTRY
from dccd.application.scheduler import Scheduler
from dccd.application.service_factory import (
    build_registry,
    build_remote,
    build_runs_store,
    build_store,
)
from dccd.domain.symbol import Symbol
from dccd.domain.timeutils import NS
from dccd.domain.types import DataType
from dccd.storage.runs_sqlite import RunsStore

_UI_DIR = pathlib.Path(__file__).parent.parent / "ui"
_TEMPLATES_DIR = _UI_DIR / "templates"
_STATIC_DIR = _UI_DIR / "static"

__all__ = ["create_app"]

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Module-level request models — FastAPI introspects these at import time;
# defining them inside create_app() would confuse its dependency resolution.
# ---------------------------------------------------------------------------

class BackfillRequest(BaseModel):
    """Request body for ``POST /api/backfill``."""

    exchange: str
    symbol: str
    data_type: str = "ohlc"
    span: int | None = None
    start: str = "last"


class StreamAction(BaseModel):
    """Request body for ``POST /api/streams/start`` and ``/stop``."""

    spec_id: str


class ReadRequest(BaseModel):
    """Request body for ``POST /api/read``."""

    exchange: str
    symbol: str
    data_type: str = "ohlc"
    span: int | None = None
    start_ns: int | None = None
    end_ns: int | None = None


class JobRunRequest(BaseModel):
    """Request body for ``POST /api/jobs/run``."""

    job_id: str


class JobCreateRequest(BaseModel):
    """Request body for ``POST /api/jobs/create``."""

    operation: str = "backfill"
    exchange: str
    symbol: str
    data_type: str = "ohlc"
    span: int | None = None
    start: str = "last"
    trigger_kind: str = "interval"
    every: int | None = None
    depth: int | None = None
    snapshot_interval: int | None = None


class JobIdRequest(BaseModel):
    """Request body for ``POST /api/jobs/delete``."""

    job_id: str


class JobUpdateRequest(BaseModel):
    """Request body for ``POST /api/jobs/update``.

    Either field may be set: ``start`` edits the first date; ``every`` sets (or,
    when ``None`` with ``schedule=True``, clears) the recurring backfill schedule.
    """

    job_id: str
    start: str | None = None
    every: int | None = None
    schedule: bool = False  # set True to apply ``every`` (incl. None = clear)


# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------

def create_app(
    config_path: str | pathlib.Path | None = None,
    config: AppConfig | None = None,
    scheduler: Scheduler | None = None,
) -> FastAPI:
    """Create and return the FastAPI application.

    Parameters
    ----------
    config_path : str or Path or None
        Path to ``config.yml``. Resolved via XDG fallback when ``None``.
    config : AppConfig or None
        Pre-loaded config. Takes precedence over *config_path*.
    scheduler : Scheduler or None
        Pass a running :class:`~dccd.application.scheduler.Scheduler` from
        ``dccd start`` so the UI controls the live daemon's jobs and streams.
        When ``None``, a standalone (non-started) scheduler is created.
    """

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        # --- startup ---
        cfg = config
        if cfg is None:
            try:
                path = resolve_config_path(config_path)
                cfg = load_config(path)
            except Exception:
                cfg = AppConfig()

        app.state.config = cfg
        app.state.config_path = config_path
        app.state.store = build_store(cfg.settings.data_path)
        app.state.runs_store = build_runs_store(cfg.settings.data_path)
        app.state.event_bus = EventBus()
        app.state.registry = build_registry()
        # Remote sync target (None when storage.remotes is empty). Resolved from
        # config — not the scheduler — so "Sync now" works in `dccd ui` mode too,
        # where the standalone scheduler has no remote wired in.
        app.state.remote = build_remote(cfg)

        if scheduler is not None:
            app.state.scheduler = scheduler
        else:
            app.state.scheduler = Scheduler(
                app.state.registry,
                app.state.store,
                app.state.runs_store,
                app.state.event_bus,
            )

        # Register stream workers from config so they can be started/stopped
        # from the UI even in standalone dccd-ui mode (without dccd start).
        stream_specs = [s for s in cfg.all_job_specs() if s.operation == "stream"]
        app.state.scheduler.register_streams(stream_specs)
        app.state.all_specs = cfg.all_job_specs()
        # Keeps strong references to background tasks so Python's GC doesn't
        # collect them mid-execution (asyncio only holds a weak ref internally).
        app.state.bg_tasks = set()
        # Per-run stop events so a running backfill can be cancelled via
        # DELETE /api/backfill/{run_id} (e.g. a runaway multi-million-row trades
        # backfill the user launched by mistake).
        app.state.backfill_stops = {}

        yield
        # --- shutdown ---

    app = FastAPI(title="dccd v3", version="3.0.0", lifespan=lifespan)

    # CORS: no wildcard. The UI is served same-origin, so it needs no CORS at
    # all; allowing every origin let any website's JS drive the local API
    # (reachable on 127.0.0.1 from the user's browser). Cross-origin access is
    # opt-in via settings.ui_allow_origins.
    allow_origins = list(getattr(getattr(config, "settings", None), "ui_allow_origins", []) or [])
    if allow_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=allow_origins,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    @app.middleware("http")
    async def _auth_guard(request: Request, call_next):
        """Require a Bearer token on /api/* when settings.ui_auth_token is set.

        Page routes are intentionally not gated (browsers can't send Bearer on
        navigation); for untrusted networks, front the UI with a reverse proxy.
        """
        cfg = getattr(request.app.state, "config", None)
        token = getattr(getattr(cfg, "settings", None), "ui_auth_token", None)
        if (
            token
            and request.url.path.startswith("/api/")
            and request.method != "OPTIONS"
        ):
            # Header for normal calls; query param for EventSource (SSE), which
            # cannot set Authorization headers.
            bearer = request.headers.get("Authorization") == f"Bearer {token}"
            query = request.query_params.get("token") == token
            if not (bearer or query):
                return JSONResponse({"detail": "Unauthorized"}, status_code=401)
        return await call_next(request)

    if _STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

    templates: Jinja2Templates | None = (
        Jinja2Templates(directory=str(_TEMPLATES_DIR)) if _TEMPLATES_DIR.exists() else None
    )

    # -- state accessors (read from app.state via request) --

    def _spawn(request: Request, coro: "Coroutine[Any, Any, Any]") -> "asyncio.Task[Any]":
        """Create a background task and hold a strong reference to it."""
        task = asyncio.create_task(coro)
        bg: set[Any] = request.app.state.bg_tasks
        bg.add(task)
        task.add_done_callback(bg.discard)
        return task

    def _parse_run(run: dict[str, Any]) -> dict[str, Any]:
        """Parse JSON-encoded fields returned from SQLite."""
        import json as _json
        for key in ("progress", "log_tail"):
            if isinstance(run.get(key), str):
                try:
                    run[key] = _json.loads(run[key])
                except Exception:
                    pass
        return run

    def _cfg(request: Request) -> AppConfig:
        return cast(AppConfig, request.app.state.config)

    def _store(request: Request):
        return request.app.state.store

    def _runs(request: Request) -> RunsStore:
        return cast(RunsStore, request.app.state.runs_store)

    def _sched(request: Request) -> Scheduler:
        return cast(Scheduler, request.app.state.scheduler)

    def _reg(request: Request):
        return request.app.state.registry

    def _bus(request: Request) -> EventBus:
        return cast(EventBus, request.app.state.event_bus)

    async def _persist_and_refresh(request: Request, new_cfg: AppConfig) -> None:
        """Persist *new_cfg* to disk (if a path is set) and refresh runtime state.

        Single source of truth for config mutations (PUT /config and the job
        CRUD endpoints): writes YAML, swaps ``app.state.config``, recomputes
        ``all_specs`` and reconciles stream workers (adds new ones, stops+drops
        deleted ones) so the daemon reflects the change without a restart.
        """
        cfg_path = request.app.state.config_path
        if cfg_path:
            import yaml
            with open(cfg_path, "w") as f:
                yaml.safe_dump(new_cfg.model_dump(), f)
        request.app.state.config = new_cfg
        request.app.state.all_specs = new_cfg.all_job_specs()
        await request.app.state.scheduler.sync_streams(request.app.state.all_specs)
        await request.app.state.scheduler.sync_intervals(request.app.state.all_specs)

    def _run_backfill_tracked(request: Request, spec: JobSpec, run_id: str) -> None:
        """Spawn a backfill with a cancellable stop event registered by run_id."""
        reg = _reg(request)
        store = _store(request)
        runs_store = _runs(request)
        bus = _bus(request)
        stops = request.app.state.backfill_stops
        stop_event = asyncio.Event()
        stops[run_id] = stop_event

        async def _run() -> None:
            from dccd.application.operations import backfill
            try:
                await backfill(spec, registry=reg, store=store, runs_store=runs_store,
                               events=bus.for_run(run_id), run_id=run_id,
                               stop_event=stop_event)
            except Exception as exc:
                logger.error("Backfill task %s failed: %s", run_id, exc)
            finally:
                stops.pop(run_id, None)

        _spawn(request, _run())

    # -----------------------------------------------------------------------
    # Operations
    # -----------------------------------------------------------------------

    @app.get("/api/operations")
    async def list_operations() -> dict[str, Any]:
        """List all registered operations."""
        return {"operations": [{"name": op} for op in REGISTRY.operations]}

    # -----------------------------------------------------------------------
    # Inventory
    # -----------------------------------------------------------------------

    @app.get("/api/inventory")
    async def get_inventory(request: Request) -> dict[str, Any]:
        """Return all stored datasets."""
        return {"datasets": _store(request).inventory()}

    # -----------------------------------------------------------------------
    # Remote sync
    # -----------------------------------------------------------------------

    @app.get("/api/storage/sync")
    async def get_sync_status(request: Request) -> dict[str, Any]:
        """Remote-sync status for the Storage page.

        Reports whether remotes are configured, the last persisted ``sync`` run
        (state/time/error), the next scheduled sync ETA, and the on-disk store
        size (the "synced volume" proxy — after a successful mirror local equals
        remote).
        """
        cfg = _cfg(request)
        remotes = [r.remote for r in cfg.storage.remotes]
        configured = bool(remotes)
        interval = cfg.storage.sync_interval
        runs = _runs(request).list_runs(spec_id="remote-sync", limit=1)
        last = None
        next_eta = None
        if runs:
            r = runs[0]
            last = {
                "state": r["state"],
                "ended_at": r["ended_at"],
                "error": r["error"],
                "rows_written": r["rows_written"],
            }
            if configured and r["ended_at"]:
                next_eta = r["ended_at"] + interval * NS
        total_bytes = sum(d.get("bytes", 0) for d in _store(request).inventory())
        return {
            "configured": configured,
            "remotes": remotes,
            "sync_interval": interval,
            "last": last,
            "next_eta": next_eta,
            "bytes": total_bytes,
        }

    @app.post("/api/storage/sync")
    async def trigger_sync(request: Request) -> dict[str, Any]:
        """Trigger a remote sync now (runs in the background)."""
        remote = request.app.state.remote
        if remote is None:
            raise HTTPException(400, "no remotes configured")
        from dccd.application.operations import sync_remote

        events = request.app.state.event_bus.for_run("remote-sync")
        _spawn(request, sync_remote(remote, runs_store=_runs(request), events=events))
        return {"status": "started"}

    # -----------------------------------------------------------------------
    # Backfill
    # -----------------------------------------------------------------------

    @app.post("/api/backfill")
    async def start_backfill(body: BackfillRequest, request: Request) -> dict[str, Any]:
        """Launch a backfill job asynchronously and return its ``run_id``.

        The ``run_id`` can be polled via ``GET /api/backfill/{run_id}``.
        """
        try:
            sym = Symbol.parse(body.symbol)
        except ValueError as e:
            raise HTTPException(400, str(e))

        data_type = DataType(body.data_type)

        # Validate span for OHLC before the task starts — avoids a silent background crash.
        if data_type == DataType.OHLC and not body.span:
            raise HTTPException(400, "span is required for data_type='ohlc'")

        target = JobTarget(
            exchange=body.exchange,
            symbol=sym,
            data_type=data_type,
            span=body.span,
        )
        spec = JobSpec(
            id=JobSpec.make_id("backfill", target),
            operation="backfill",
            target=target,
            trigger=Trigger(kind="once"),
            params=JobParams(start=body.start),
            origin="runtime",
        )

        # Generate a URL-safe run_id and pass it into backfill() so both the
        # API polling endpoint and the RunsStore use the same identifier.
        # We use a short UUID (no slashes) instead of embedding spec.id which
        # may contain '/' from the symbol (e.g. BTC/USDT) and would break routing.
        import uuid as _uuid
        run_id = str(_uuid.uuid4())
        _run_backfill_tracked(request, spec, run_id)
        return {"run_id": run_id, "status": "started"}

    @app.get("/api/backfill/{run_id}")
    async def get_backfill_status(run_id: str, request: Request) -> dict[str, Any]:
        """Get the status of a backfill run by ``run_id``."""
        run = _runs(request).get_run(run_id)
        if not run:
            raise HTTPException(404, f"Run {run_id!r} not found")
        return _parse_run(run)

    @app.delete("/api/backfill/{run_id}")
    async def cancel_backfill(run_id: str, request: Request) -> dict[str, Any]:
        """Cancel a running backfill (cooperative — stops at the next page)."""
        ev = request.app.state.backfill_stops.get(run_id)
        if ev is None:
            raise HTTPException(404, f"No running backfill {run_id!r}")
        ev.set()
        return {"status": "cancelling", "run_id": run_id}

    @app.get("/api/runs")
    async def list_runs(request: Request, limit: int = 50) -> dict[str, Any]:
        """List recent job runs."""
        return {"runs": [_parse_run(r) for r in _runs(request).list_runs(limit=limit)]}

    # -----------------------------------------------------------------------
    # Stream control
    # -----------------------------------------------------------------------

    @app.get("/api/streams")
    async def list_streams(request: Request) -> dict[str, Any]:
        """List stream jobs and their running state."""
        sched = _sched(request)
        return {"streams": [
            {"id": sid, "running": running}
            for sid, running in sched.stream_status().items()
        ]}

    @app.post("/api/streams/start")
    async def start_stream(body: StreamAction, request: Request) -> dict[str, Any]:
        """Start a supervised stream job."""
        if not _sched(request).start_stream(body.spec_id):
            raise HTTPException(404, f"Stream job {body.spec_id!r} not found")
        return {"status": "started"}

    @app.post("/api/streams/stop")
    async def stop_stream(body: StreamAction, request: Request) -> dict[str, Any]:
        """Stop a supervised stream job."""
        if not await _sched(request).stop_stream(body.spec_id):
            raise HTTPException(404, f"Stream job {body.spec_id!r} not found")
        return {"status": "stopped"}

    # -----------------------------------------------------------------------
    # Jobs
    # -----------------------------------------------------------------------

    @app.get("/api/jobs")
    async def list_jobs(request: Request) -> dict[str, Any]:
        """List all configured job specs and their current state."""
        specs = getattr(request.app.state, "all_specs", _cfg(request).all_job_specs())
        stream_status = _sched(request).stream_status()
        return {"jobs": [
            {
                "id": s.id,
                "operation": s.operation,
                "exchange": s.target.exchange,
                "symbol": str(s.target.symbol),
                "data_type": s.target.data_type.value,
                "span": s.target.span,
                "trigger": s.trigger.kind,
                "every": s.trigger.every,
                "start": s.params.start,
                "snapshot_interval": s.params.snapshot_interval,
                "depth": s.params.depth,
                "enabled": s.enabled,
                "running": stream_status.get(s.id, False) if s.operation == "stream" else None,
            }
            for s in specs
        ]}

    @app.post("/api/jobs/run")
    async def run_job_now(body: JobRunRequest, request: Request) -> dict[str, Any]:
        """Trigger an immediate one-shot backfill for a configured job.

        Uses a POST body to avoid URL-routing issues with job IDs that contain
        slashes (e.g. ``backfill:binance:BTC/USDT:ohlc:3600s``).
        """
        import uuid as _uuid
        job_id = body.job_id
        specs = getattr(request.app.state, "all_specs", _cfg(request).all_job_specs())
        spec = next((s for s in specs if s.id == job_id), None)
        if spec is None:
            raise HTTPException(404, f"Job {job_id!r} not found")
        if spec.operation != "backfill":
            raise HTTPException(400, "Only backfill jobs can be triggered manually; use /api/streams/start for stream jobs")

        run_id = str(_uuid.uuid4())
        _run_backfill_tracked(request, spec, run_id)

        return {"run_id": run_id, "status": "started", "job_id": job_id}

    @app.post("/api/jobs/run-all")
    async def run_all_backfill_jobs(request: Request) -> dict[str, Any]:
        """Trigger an immediate backfill for all enabled backfill jobs."""
        import uuid as _uuid
        specs = getattr(request.app.state, "all_specs", _cfg(request).all_job_specs())
        backfill_specs = [s for s in specs if s.operation == "backfill" and s.enabled]

        run_ids = []
        for spec in backfill_specs:
            run_id = str(_uuid.uuid4())
            run_ids.append({"run_id": run_id, "job_id": spec.id})
            _run_backfill_tracked(request, spec, run_id)

        return {"started": len(run_ids), "runs": run_ids}

    # -----------------------------------------------------------------------
    # Config
    # -----------------------------------------------------------------------

    @app.get("/api/config")
    async def get_config(request: Request) -> dict[str, Any]:
        """Return the current configuration as a dict."""
        return _cfg(request).model_dump()

    @app.put("/api/config")
    async def update_config(request: Request) -> dict[str, Any]:
        """Replace the configuration; persists to disk when *config_path* is set."""
        body = await request.json()
        try:
            new_cfg = AppConfig.model_validate(body)
        except Exception as e:
            raise HTTPException(422, str(e))

        await _persist_and_refresh(request, new_cfg)
        return {"status": "ok"}

    @app.post("/api/jobs/create")
    async def create_job(body: JobCreateRequest, request: Request) -> dict[str, Any]:
        """Create a new job (backfill or stream) and persist it to config."""
        try:
            sym = Symbol.parse(body.symbol)
        except ValueError as e:
            raise HTTPException(400, str(e))
        new_cfg = _cfg(request).model_copy(deep=True)
        try:
            job_id = new_cfg.add_job(
                operation=body.operation,
                exchange=body.exchange,
                pair=str(sym),
                data_type=body.data_type,
                span=body.span,
                start=body.start,
                trigger_kind=body.trigger_kind,
                every=body.every,
                depth=body.depth,
                snapshot_interval=body.snapshot_interval,
            )
        except ValueError as e:
            raise HTTPException(400, str(e))
        await _persist_and_refresh(request, new_cfg)
        return {"status": "created", "job_id": job_id}

    @app.post("/api/jobs/delete")
    async def delete_job(body: JobIdRequest, request: Request) -> dict[str, Any]:
        """Delete a configured job (does NOT delete stored data)."""
        new_cfg = _cfg(request).model_copy(deep=True)
        if not new_cfg.remove_job(body.job_id):
            raise HTTPException(404, f"Job {body.job_id!r} not found")
        await _persist_and_refresh(request, new_cfg)
        return {"status": "deleted", "job_id": body.job_id}

    @app.post("/api/jobs/update")
    async def update_job(body: JobUpdateRequest, request: Request) -> dict[str, Any]:
        """Update a job's first date and/or recurring schedule, then persist."""
        new_cfg = _cfg(request).model_copy(deep=True)
        found = False
        try:
            if body.start is not None:
                found = new_cfg.update_job_start(body.job_id, body.start) or found
            if body.schedule:
                found = new_cfg.update_job_schedule(body.job_id, body.every) or found
        except ValueError as e:
            raise HTTPException(400, str(e))
        if not found:
            raise HTTPException(404, f"Job {body.job_id!r} not found")
        await _persist_and_refresh(request, new_cfg)
        return {"status": "updated", "job_id": body.job_id}

    # -----------------------------------------------------------------------
    # Read
    # -----------------------------------------------------------------------

    @app.post("/api/read")
    async def read_data(body: ReadRequest, request: Request) -> dict[str, Any]:
        """Read stored data for a dataset (returns at most 1 000 rows)."""
        from dccd.application.operations import read
        try:
            sym = Symbol.parse(body.symbol)
        except ValueError as e:
            raise HTTPException(400, str(e))
        target = JobTarget(
            exchange=body.exchange, symbol=sym,
            data_type=DataType(body.data_type), span=body.span,
        )
        df = read(target, store=_store(request), start_ns=body.start_ns, end_ns=body.end_ns)
        rows = df.to_dicts() if hasattr(df, "to_dicts") else []
        return {"rows": len(rows), "data": rows[:1000]}

    # -----------------------------------------------------------------------
    # SSE events
    # -----------------------------------------------------------------------

    @app.get("/api/events")
    async def sse_events(request: Request) -> StreamingResponse:
        """Server-Sent Events stream of progress/log/status events."""
        bus = _bus(request)
        queue = bus.add_queue()

        async def _generator():
            try:
                while True:
                    if await request.is_disconnected():
                        break
                    try:
                        event = await asyncio.wait_for(queue.get(), timeout=30.0)
                        yield f"data: {event.model_dump_json()}\n\n"
                    except asyncio.TimeoutError:
                        yield ": heartbeat\n\n"
            finally:
                bus.remove_queue(queue)

        return StreamingResponse(_generator(), media_type="text/event-stream")

    # -----------------------------------------------------------------------
    # Health
    # -----------------------------------------------------------------------

    @app.get("/health")
    async def health() -> dict[str, str]:
        """Liveness check."""
        return {"status": "ok"}

    # -----------------------------------------------------------------------
    # UI pages
    # -----------------------------------------------------------------------

    if templates is not None:
        from importlib.metadata import version as _pkg_version

        def _tpl_ctx(request: Request, page: str) -> dict[str, Any]:
            # request is NOT included here — Starlette 1.x injects it automatically
            # when using the new TemplateResponse(request, name, context) signature.
            try:
                ver = _pkg_version("dccd")
            except Exception:
                ver = "dev"
            cfg = getattr(request.app.state, "config", None)
            settings = getattr(cfg, "settings", None)
            token = getattr(settings, "ui_auth_token", None)
            tz = getattr(settings, "timezone", None) or "local"
            return {
                "active": request.url.path, "version": ver, "page": page,
                "auth_token": token or "", "timezone": tz,
            }

        # Starlette >= 0.29 / 1.x signature: TemplateResponse(request, name, context)
        @app.get("/")
        async def ui_dashboard(request: Request):
            return templates.TemplateResponse(request, "dashboard.html", _tpl_ctx(request, "dashboard"))

        @app.get("/data")
        async def ui_data(request: Request):
            return templates.TemplateResponse(request, "data.html", _tpl_ctx(request, "data"))

        @app.get("/inventory")
        async def ui_inventory() -> RedirectResponse:
            # Page renamed Inventory → Data; keep the old path working.
            return RedirectResponse("/data", status_code=307)

        @app.get("/historical")
        async def ui_historical(request: Request):
            return templates.TemplateResponse(request, "historical.html", _tpl_ctx(request, "historical"))

        @app.get("/live")
        async def ui_live(request: Request):
            return templates.TemplateResponse(request, "live.html", _tpl_ctx(request, "live"))

        @app.get("/config")
        async def ui_config(request: Request):
            return templates.TemplateResponse(request, "config.html", _tpl_ctx(request, "config"))

        @app.get("/logs")
        async def ui_logs(request: Request):
            return templates.TemplateResponse(request, "logs.html", _tpl_ctx(request, "logs"))

        @app.get("/storage")
        async def ui_storage(request: Request):
            return templates.TemplateResponse(request, "storage.html", _tpl_ctx(request, "storage"))

    return app
