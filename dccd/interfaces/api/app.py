"""FastAPI application — 1:1 with the Operation Registry + SSE events."""

from __future__ import annotations

import asyncio
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
from dccd.domain.symbol import Symbol
from dccd.domain.types import DataType
from dccd.sources.registry import SourceRegistry
from dccd.storage.parquet import ParquetStore
from dccd.storage.runs_sqlite import RunsStore

_UI_DIR = pathlib.Path(__file__).parent.parent / "ui"
_TEMPLATES_DIR = _UI_DIR / "templates"
_STATIC_DIR = _UI_DIR / "static"

__all__ = ["create_app"]

logger = logging.getLogger(__name__)


# Module-level request models — FastAPI requires these at module scope for body parsing
class BackfillRequest(BaseModel):
    exchange: str
    symbol: str
    data_type: str = "ohlc"
    span: int | None = None
    start: str = "last"
    parallel: bool = False


class StreamAction(BaseModel):
    spec_id: str


class ReadRequest(BaseModel):
    exchange: str
    symbol: str
    data_type: str = "ohlc"
    span: int | None = None
    start_ns: int | None = None
    end_ns: int | None = None


class MigrateRequest(BaseModel):
    dry_run: bool = True


def _build_registry(config: AppConfig) -> SourceRegistry:
    from dccd.sources.binance import BinanceSource
    from dccd.sources.bitfinex import BitfinexSource
    from dccd.sources.bitmex import BitMEXSource
    from dccd.sources.bybit import BybitSource
    from dccd.sources.coinbase import CoinbaseSource
    from dccd.sources.kraken import KrakenSource
    from dccd.sources.okx import OKXSource

    reg = SourceRegistry()
    reg.register("binance", BinanceSource())
    reg.register("coinbase", CoinbaseSource())
    reg.register("kraken", KrakenSource())
    reg.register("bybit", BybitSource())
    reg.register("okx", OKXSource())
    reg.register("bitfinex", BitfinexSource())
    reg.register("bitmex", BitMEXSource())
    return reg


def create_app(
    config_path: str | pathlib.Path | None = None,
    config: AppConfig | None = None,
    scheduler: Scheduler | None = None,
) -> FastAPI:
    """Create the FastAPI application.

    Parameters
    ----------
    config_path : str or Path or None
    config : AppConfig or None
    scheduler : Scheduler or None
        Pass a running Scheduler from ``dccd start`` to control jobs from UI.
    """
    app = FastAPI(title="dccd v3", version="3.0.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    if _STATIC_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

    templates: Jinja2Templates | None = None
    if _TEMPLATES_DIR.exists():
        templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))

    @app.on_event("startup")
    async def startup() -> None:
        cfg = config
        if cfg is None:
            try:
                path = resolve_config_path(config_path)
                cfg = load_config(path)
            except Exception:
                cfg = AppConfig()

        app.state.config = cfg
        app.state.config_path = config_path
        app.state.store = ParquetStore(cfg.settings.data_path)
        app.state.runs_store = RunsStore(
            pathlib.Path(cfg.settings.data_path) / ".dccd" / "runs.db"
        )
        app.state.event_bus = EventBus()
        app.state.registry = _build_registry(cfg)

        if scheduler is not None:
            app.state.scheduler = scheduler
        else:
            app.state.scheduler = Scheduler(
                app.state.registry,
                app.state.store,
                app.state.runs_store,
                app.state.event_bus,
            )

    def _cfg(request: Request) -> AppConfig:
        return request.app.state.config

    def _store(request: Request) -> ParquetStore:
        return request.app.state.store

    def _runs(request: Request) -> RunsStore:
        return request.app.state.runs_store

    def _sched(request: Request) -> Scheduler:
        return request.app.state.scheduler

    def _reg(request: Request) -> SourceRegistry:
        return request.app.state.registry

    def _bus(request: Request) -> EventBus:
        return request.app.state.event_bus

    # -----------------------------------------------------------------------
    # Operations — /api/operations
    # -----------------------------------------------------------------------

    @app.get("/api/operations")
    async def list_operations() -> dict[str, Any]:
        return {"operations": [{"name": op} for op in REGISTRY.operations]}

    # -----------------------------------------------------------------------
    # Inventory — /api/inventory
    # -----------------------------------------------------------------------

    @app.get("/api/inventory")
    async def get_inventory(request: Request) -> dict[str, Any]:
        return {"datasets": _store(request).inventory()}

    # -----------------------------------------------------------------------
    # Backfill — /api/backfill
    # -----------------------------------------------------------------------

    @app.post("/api/backfill")
    async def start_backfill(body: BackfillRequest, request: Request) -> dict[str, Any]:
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

        async def _run() -> None:
            from dccd.application.operations import backfill
            evts = _bus(request).for_run(run_id)
            await backfill(spec, registry=_reg(request), store=_store(request),
                           runs_store=_runs(request), events=evts)

        asyncio.create_task(_run())
        return {"run_id": run_id, "status": "started"}

    @app.get("/api/backfill/{run_id}")
    async def get_backfill_status(run_id: str, request: Request) -> dict[str, Any]:
        run = _runs(request).get_run(run_id)
        if not run:
            raise HTTPException(404, f"Run {run_id!r} not found")
        return run

    @app.get("/api/runs")
    async def list_runs(request: Request, limit: int = 50) -> dict[str, Any]:
        return {"runs": _runs(request).list_runs(limit=limit)}

    # -----------------------------------------------------------------------
    # Stream control — /api/streams
    # -----------------------------------------------------------------------

    @app.get("/api/streams")
    async def list_streams(request: Request) -> dict[str, Any]:
        sched = _sched(request)
        return {"streams": [
            {"id": sid, "running": running}
            for sid, running in sched.stream_status().items()
        ]}

    @app.post("/api/streams/start")
    async def start_stream(body: StreamAction, request: Request) -> dict[str, Any]:
        ok = _sched(request).start_stream(body.spec_id)
        if not ok:
            raise HTTPException(404, f"Stream job {body.spec_id!r} not found")
        return {"status": "started"}

    @app.post("/api/streams/stop")
    async def stop_stream(body: StreamAction, request: Request) -> dict[str, Any]:
        ok = await _sched(request).stop_stream(body.spec_id)
        if not ok:
            raise HTTPException(404, f"Stream job {body.spec_id!r} not found")
        return {"status": "stopped"}

    # -----------------------------------------------------------------------
    # Jobs — /api/jobs
    # -----------------------------------------------------------------------

    @app.get("/api/jobs")
    async def list_jobs(request: Request) -> dict[str, Any]:
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
    # Config — /api/config
    # -----------------------------------------------------------------------

    @app.get("/api/config")
    async def get_config(request: Request) -> dict[str, Any]:
        return _cfg(request).model_dump()

    @app.put("/api/config")
    async def update_config(request: Request) -> dict[str, Any]:
        body = await request.json()
        from dccd.application.config import AppConfig
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
    # Read — /api/read
    # -----------------------------------------------------------------------

    @app.post("/api/read")
    async def read_data(body: ReadRequest, request: Request) -> dict[str, Any]:
        from dccd.application.operations import read
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
        df = read(target, store=_store(request), start_ns=body.start_ns, end_ns=body.end_ns)
        if hasattr(df, "to_dicts"):
            rows = df.to_dicts()
        else:
            rows = []
        return {"rows": len(rows), "data": rows[:1000]}

    # -----------------------------------------------------------------------
    # Events SSE — /api/events
    # -----------------------------------------------------------------------

    @app.get("/api/events")
    async def sse_events(request: Request) -> StreamingResponse:
        queue = _bus(request).enable_queue()

        async def generator():
            while True:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=30.0)
                    data = event.model_dump_json()
                    yield f"data: {data}\n\n"
                except asyncio.TimeoutError:
                    yield ": heartbeat\n\n"

        return StreamingResponse(generator(), media_type="text/event-stream")

    # -----------------------------------------------------------------------
    # Migration — /api/migrate
    # -----------------------------------------------------------------------

    @app.post("/api/migrate")
    async def migrate_data(body: MigrateRequest, request: Request) -> dict[str, Any]:
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
        return {"status": "ok"}

    # -----------------------------------------------------------------------
    # UI pages
    # -----------------------------------------------------------------------

    if templates is not None:
        from importlib.metadata import version as pkg_version

        def _tpl_ctx(request: Request, page: str) -> dict:
            try:
                ver = pkg_version("dccd")
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
