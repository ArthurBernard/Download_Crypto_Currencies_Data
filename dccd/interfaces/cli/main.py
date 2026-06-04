"""dccd v3 CLI — Typer commands mapped 1:1 on the Operation Registry."""

from __future__ import annotations

import asyncio
import pathlib
from typing import Optional

import typer

app = typer.Typer(name="dccd", help="Download Crypto Currency Data — v3")


def _load_cfg(config: Optional[pathlib.Path]):
    from dccd.application.config import load_config, resolve_config_path
    path = resolve_config_path(config)
    return load_config(path), path


@app.command("validate")
def validate(
    config: Optional[pathlib.Path] = typer.Option(None, "--config", "-c", help="Config file path"),
) -> None:
    """Validate the configuration file."""
    try:
        cfg, path = _load_cfg(config)
        typer.echo(f"✓ Config valid: {path}")
        specs = cfg.all_job_specs()
        typer.echo(f"  {len(specs)} job spec(s) defined")
    except Exception as exc:
        typer.echo(f"✗ Config invalid: {exc}", err=True)
        raise typer.Exit(1)


@app.command("backfill")
def cmd_backfill(
    config: Optional[pathlib.Path] = typer.Option(None, "--config", "-c"),
    exchange: Optional[str] = typer.Option(None, "--exchange", "-e"),
    symbol: Optional[str] = typer.Option(None, "--symbol", "-s"),
    data_type: str = typer.Option("ohlc", "--type", "-t"),
    span: Optional[int] = typer.Option(None, "--span"),
    start: str = typer.Option("last", "--start"),
) -> None:
    """Backfill historical data for one or all configured jobs."""
    from dccd.application.jobs import JobParams, JobSpec, JobTarget, Trigger
    from dccd.application.operations import backfill as do_backfill
    from dccd.application.service_factory import (
        build_registry,
        build_runs_store,
        build_store,
    )
    from dccd.domain.symbol import Symbol
    from dccd.domain.types import DataType

    cfg, _ = _load_cfg(config)
    store = build_store(cfg.settings.data_path)
    runs_store = build_runs_store(cfg.settings.data_path)
    registry = build_registry()

    specs: list[JobSpec] = []
    if exchange and symbol:
        sym = Symbol.parse(symbol)
        target = JobTarget(exchange=exchange, symbol=sym, data_type=DataType(data_type), span=span)
        specs = [JobSpec(
            id=JobSpec.make_id("backfill", target),
            operation="backfill",
            target=target,
            trigger=Trigger(kind="once"),
            params=JobParams(start=start),
            origin="runtime",
        )]
    else:
        specs = [s for s in cfg.all_job_specs() if s.operation == "backfill"]
        if exchange:
            specs = [s for s in specs if s.target.exchange == exchange]

    if not specs:
        typer.echo("No backfill jobs found.")
        raise typer.Exit(1)

    async def _run():
        for spec in specs:
            typer.echo(f"Backfilling {spec.id}…")
            result = await do_backfill(spec, registry=registry, store=store, runs_store=runs_store)
            if err := result.get("error"):
                typer.echo(f"  ✗ {err}", err=True)
            else:
                typer.echo(f"  ✓ {result.get('rows_written', 0)} rows written")

    asyncio.run(_run())


@app.command("stream")
def cmd_stream(
    config: Optional[pathlib.Path] = typer.Option(None, "--config", "-c"),
) -> None:
    """Start WebSocket stream jobs defined in config."""
    from dccd.application.events import EventBus
    from dccd.application.scheduler import Scheduler
    from dccd.application.service_factory import (
        build_registry,
        build_runs_store,
        build_store,
    )

    cfg, _ = _load_cfg(config)
    store = build_store(cfg.settings.data_path)
    runs_store = build_runs_store(cfg.settings.data_path)
    registry = build_registry()
    bus = EventBus()
    scheduler = Scheduler(registry, store, runs_store, bus)

    specs = [s for s in cfg.all_job_specs() if s.operation == "stream"]
    if not specs:
        typer.echo("No stream jobs in config.")
        raise typer.Exit(1)

    typer.echo(f"Starting {len(specs)} stream job(s)… (Ctrl-C to stop)")

    async def _run():
        await scheduler.start(specs)
        try:
            while True:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        finally:
            await scheduler.stop()

    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        typer.echo("\nStopped.")


@app.command("start")
def cmd_start(
    config: Optional[pathlib.Path] = typer.Option(None, "--config", "-c"),
    host: Optional[str] = typer.Option(None, "--host"),
    port: Optional[int] = typer.Option(None, "--port"),
) -> None:
    """Start the full daemon (scheduler + streams + web UI)."""
    import uvicorn

    from dccd.application.events import EventBus
    from dccd.application.scheduler import Scheduler
    from dccd.application.service_factory import (
        build_registry,
        build_runs_store,
        build_store,
    )
    from dccd.interfaces.api.app import create_app

    cfg, cfg_path = _load_cfg(config)
    store = build_store(cfg.settings.data_path)
    runs_store = build_runs_store(cfg.settings.data_path)
    registry = build_registry()
    bus = EventBus()
    scheduler = Scheduler(registry, store, runs_store, bus)

    # Run *all* configured jobs, not just streams: the scheduler routes each by
    # trigger kind (supervised → stream worker; interval/cron → periodic
    # backfill; once → one-shot). Passing only streams meant configured
    # scheduled backfills never ran under `dccd start`.
    all_specs = cfg.all_job_specs()
    fastapi_app = create_app(config_path=cfg_path, config=cfg, scheduler=scheduler)

    ui_host = host or cfg.settings.ui_host
    ui_port = port or cfg.settings.ui_port

    async def _run():
        await scheduler.start(all_specs)
        typer.echo(f"Daemon running — UI at http://{ui_host}:{ui_port}")
        server = uvicorn.Server(uvicorn.Config(fastapi_app, host=ui_host, port=ui_port, log_level="warning"))
        try:
            await server.serve()
        finally:
            await scheduler.stop()

    asyncio.run(_run())


@app.command("status")
def cmd_status(
    config: Optional[pathlib.Path] = typer.Option(None, "--config", "-c"),
) -> None:
    """Show recent job runs from the runs database."""
    from dccd.application.service_factory import build_runs_store

    cfg, _ = _load_cfg(config)
    runs_store = build_runs_store(cfg.settings.data_path)
    runs = runs_store.list_runs(limit=10)
    if not runs:
        typer.echo("No runs found.")
        return
    for r in runs:
        typer.echo(f"{r['run_id']}: {r['state']} ({r.get('rows_written', 0)} rows)")


@app.command("inventory")
def cmd_inventory(
    config: Optional[pathlib.Path] = typer.Option(None, "--config", "-c"),
) -> None:
    """List all stored datasets."""
    from dccd.application.service_factory import build_store

    cfg, _ = _load_cfg(config)
    store = build_store(cfg.settings.data_path)
    datasets = store.inventory()
    if not datasets:
        typer.echo("No datasets found.")
        return
    for d in datasets:
        parts = [d["exchange"], d["pair"], d["data_type"]]
        if d.get("span") is not None:
            parts.append(f"{d['span']}s")
        typer.echo(f"  {' / '.join(parts)}  ({d['files']} file(s))")


@app.command("migrate")
def cmd_migrate(
    config: Optional[pathlib.Path] = typer.Option(None, "--config", "-c"),
    dry_run: bool = typer.Option(True, "--dry-run/--no-dry-run",
                                  help="Preview changes without writing."),
) -> None:
    """Migrate existing Parquet files from second-scale to nanosecond timestamps."""
    from dccd.storage.migrate import migrate_parquet_to_ns

    cfg, _ = _load_cfg(config)
    report = migrate_parquet_to_ns(cfg.settings.data_path, dry_run=dry_run)
    migrated = sum(1 for r in report if r.get("migrated"))
    prefix = "[dry-run] " if dry_run else ""
    typer.echo(f"{prefix}{migrated}/{len(report)} files migrated")


@app.command("ui")
def cmd_ui(
    config: Optional[pathlib.Path] = typer.Option(None, "--config", "-c"),
    host: Optional[str] = typer.Option(None, "--host"),
    port: Optional[int] = typer.Option(None, "--port"),
) -> None:
    """Start only the web UI (API + static files, no scheduler)."""
    import uvicorn

    from dccd.interfaces.api.app import create_app

    cfg, cfg_path = _load_cfg(config)
    fastapi_app = create_app(config_path=cfg_path, config=cfg)
    ui_host = host or cfg.settings.ui_host
    ui_port = port or cfg.settings.ui_port
    typer.echo(f"UI at http://{ui_host}:{ui_port}")
    uvicorn.run(fastapi_app, host=ui_host, port=ui_port)
