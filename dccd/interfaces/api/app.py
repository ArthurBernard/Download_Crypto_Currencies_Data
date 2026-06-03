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
import time
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
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
    build_runs_store,
    build_store,
)
from dccd.domain.symbol import Symbol
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
    parallel: bool = False


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


class MigrateRequest(BaseModel):
    """Request body for ``POST /api/migrate``."""

    dry_run: bool = True


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

        if scheduler is not None:
            app.state.scheduler = scheduler
        else:
            app.state.scheduler = Scheduler(
                app.state.registry,
                app.state.store,
                app.state.runs_store,
                app.state.event_bus,
            )

        yield
        # --- shutdown (nothing to clean up for now) ---

    app = FastAPI(title="dccd v3", version="3.0.0", lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    if _STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

    templates: Jinja2Templates | None = (
        Jinja2Templates(directory=str(_TEMPLATES_DIR)) if _TEMPLATES_DIR.exists() else None
    )

    # -- state accessors (read from app.state via request) --

    def _cfg(request: Request) -> AppConfig:
        return request.app.state.config

    def _store(request: Request):
        return request.app.state.store

    def _runs(request: Request) -> RunsStore:
        return request.app.state.runs_store

    def _sched(request: Request) -> Scheduler:
        return request.app.state.scheduler

    def _reg(request: Request):
        return request.app.state.registry

    def _bus(request: Request) -> EventBus:
        return request.app.state.event_bus

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
    # Backfill
    # -----------------------------------------------------------------------

    @app.post("/api/backfill")
    async def start_backfill(body: BackfillRequest, request: Request) -> dict[str, Any]:
        """Launch a backfill job asynchronously and return its ``run_id``."""
        try:
            sym = Symbol.parse(body.symbol)
        except ValueError as e:
            raise HTTPException(400, str(e))

        target = JobTarget(
            exchange=body.exchange,
            symbol=sym,
            data_type=DataType(body.data_type),
            span=body.span,
        )
        spec = JobSpec(
            id=JobSpec.make_id("backfill", target),
            operation="backfill",
            target=target,
            trigger=Trigger(kind="once"),
            params=JobParams(start=body.start, parallel=body.parallel),  # type: ignore[arg-type]
            origin="runtime",
        )
        run_id = f"backfill:{body.exchange}:{body.symbol}@{int(time.time())}"

        # Capture state objects before creating the task — the Request object
        # is recycled by Starlette after the response is sent.
        reg = _reg(request)
        store = _store(request)
        runs_store = _runs(request)
        bus = _bus(request)

        async def _run() -> None:
            from dccd.application.operations import backfill
            evts = bus.for_run(run_id)
            await backfill(spec, registry=reg, store=store, runs_store=runs_store, events=evts)

        asyncio.create_task(_run())
        return {"run_id": run_id, "status": "started"}

    @app.get("/api/backfill/{run_id}")
    async def get_backfill_status(run_id: str, request: Request) -> dict[str, Any]:
        """Get the status of a backfill run by ``run_id``."""
        run = _runs(request).get_run(run_id)
        if not run:
            raise HTTPException(404, f"Run {run_id!r} not found")
        return run

    @app.get("/api/runs")
    async def list_runs(request: Request, limit: int = 50) -> dict[str, Any]:
        """List recent job runs."""
        return {"runs": _runs(request).list_runs(limit=limit)}

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
        specs = _cfg(request).all_job_specs()
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
                "enabled": s.enabled,
                "running": stream_status.get(s.id, False) if s.operation == "stream" else None,
            }
            for s in specs
        ]}

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

        cfg_path = request.app.state.config_path
        if cfg_path:
            import yaml
            with open(cfg_path, "w") as f:
                yaml.safe_dump(new_cfg.model_dump(), f)

        request.app.state.config = new_cfg
        return {"status": "ok"}

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
        queue = _bus(request).enable_queue()

        async def _generator():
            while True:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=30.0)
                    yield f"data: {event.model_dump_json()}\n\n"
                except asyncio.TimeoutError:
                    yield ": heartbeat\n\n"

        return StreamingResponse(_generator(), media_type="text/event-stream")

    # -----------------------------------------------------------------------
    # Migration
    # -----------------------------------------------------------------------

    @app.post("/api/migrate")
    async def migrate_data(body: MigrateRequest, request: Request) -> dict[str, Any]:
        """Migrate existing Parquet files from seconds to nanosecond timestamps."""
        from dccd.storage.migrate import migrate_parquet_to_ns
        report = await asyncio.to_thread(
            migrate_parquet_to_ns,
            _cfg(request).settings.data_path,
            dry_run=body.dry_run,
        )
        return {"report": report}

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

        def _tpl_ctx(request: Request, page: str) -> dict:
            try:
                ver = _pkg_version("dccd")
            except Exception:
                ver = "dev"
            return {"request": request, "active": request.url.path, "version": ver, "page": page}

        @app.get("/")
        async def ui_dashboard(request: Request):
            return templates.TemplateResponse("dashboard.html", _tpl_ctx(request, "dashboard"))

        @app.get("/inventory")
        async def ui_inventory(request: Request):
            return templates.TemplateResponse("inventory.html", _tpl_ctx(request, "inventory"))

        @app.get("/jobs")
        async def ui_jobs(request: Request):
            return templates.TemplateResponse("jobs.html", _tpl_ctx(request, "jobs"))

        @app.get("/config")
        async def ui_config(request: Request):
            return templates.TemplateResponse("config.html", _tpl_ctx(request, "config"))

        @app.get("/logs")
        async def ui_logs(request: Request):
            return templates.TemplateResponse("logs.html", _tpl_ctx(request, "logs"))

        @app.get("/storage")
        async def ui_storage(request: Request):
            return templates.TemplateResponse("storage.html", _tpl_ctx(request, "storage"))

    return app
