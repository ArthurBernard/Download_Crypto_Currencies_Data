#!/usr/bin/env python3
# coding: utf-8

""" Command-line interface for the dccd daemon.

.. currentmodule:: dccd.daemon.cli

Entry point installed by ``pyproject.toml [project.scripts]``:

.. code-block:: bash

    pip install "dccd[daemon]"
    dccd --help

Commands
--------

.. code-block:: text

    dccd validate --config PATH
        Parse and validate the YAML config; print a one-line summary.
        Exit 0 on success, 1 on any error (file not found, bad YAML,
        Pydantic validation failure).

    dccd run --config PATH
        Execute every histo_job once in order, then exit.
        Metrics (success/failure counts) are printed on completion.
        Useful for cron-based one-shot collection or smoke-testing a config.

    dccd start --config PATH
        Start the continuous daemon in the foreground:
        - APScheduler BackgroundScheduler for all histo_jobs
        - StreamManager (one thread per WebSocket pair)
        - SyncService (periodic rclone push to remotes)
        Block until SIGINT (Ctrl-C) or SIGTERM; shuts down cleanly on signal.

    dccd status --config PATH
        Read {local_path}/.dccd/metrics.json and render a table:

            job                      last_run          last_success      rows  errors
            -----------------------------------------------------------------------
            binance/BTC/USDT         2026-05-17 10:00  2026-05-17 10:00  1200       0
            kraken/ETH/USD           2026-05-17 09:58  2026-05-17 09:30   800       3

    dccd add --exchange X --pair Y --span N [--config PATH]
        Append a new histo_job to the YAML config file in-place and
        re-validate the modified config before writing.

"""

from __future__ import annotations

import json
import signal
import threading
from datetime import datetime, timezone
from pathlib import Path

import typer

__all__ = ['app']

app = typer.Typer(help='dccd — autonomous crypto data collection daemon')

_DEFAULT_CONFIG = 'config.yml'


def _load(config_path: str) -> object:
    """Load config, exit with code 1 on any error."""
    from dccd.daemon.config import load_config

    try:
        return load_config(config_path)
    except FileNotFoundError:
        typer.echo(f'Error: config file not found: {config_path}', err=True)
        raise typer.Exit(1)
    except Exception as exc:
        typer.echo(f'Error: {exc}', err=True)
        raise typer.Exit(1)


@app.command()
def validate(
    config: str = typer.Option(_DEFAULT_CONFIG, '--config', '-c',
                               help='Path to the YAML config file.'),
) -> None:
    """ Validate a YAML config file and print a one-line summary.

    Loads the file, runs Pydantic validation, and prints a count of
    histo_jobs, stream_jobs, remotes, and the local storage path.
    Exits with code 1 on any error (missing file, bad YAML, invalid config).

    """
    cfg = _load(config)
    typer.echo(f'Config: {config}')
    typer.echo(f'  storage.local_path : {cfg.storage.local_path}')  # type: ignore[attr-defined]
    typer.echo(f'  remotes            : {len(cfg.storage.remotes)}')  # type: ignore[attr-defined]
    typer.echo(f'  histo_jobs         : {len(cfg.histo_jobs)}')  # type: ignore[attr-defined]
    typer.echo(f'  stream_jobs        : {len(cfg.stream_jobs)}')  # type: ignore[attr-defined]
    typer.echo('Config is valid.')


@app.command()
def run(
    config: str = typer.Option(_DEFAULT_CONFIG, '--config', '-c',
                               help='Path to the YAML config file.'),
) -> None:
    """ Run every histo_job once sequentially, then exit.

    Downloads and saves one candle batch per ``(exchange, pair)`` in
    ``histo_jobs``.  A :class:`~dccd.daemon.health.HealthMonitor` is
    instantiated so metrics are persisted even for this one-shot run.
    Failed jobs are logged and skipped; remaining jobs continue.
    Prints ``successes=N failures=M`` on completion.

    """
    from dccd.daemon.health import HealthMonitor
    from dccd.daemon.scheduler import run_once

    cfg = _load(config)
    health = HealthMonitor(cfg.storage.local_path, cfg.alerts)  # type: ignore[attr-defined]
    run_once(cfg, health=health)  # type: ignore[arg-type]
    metrics = health.get_metrics()
    successes = sum(1 for m in metrics.values() if m.errors_count == 0)
    failures = sum(1 for m in metrics.values() if m.errors_count > 0)
    typer.echo(f'Done. successes={successes} failures={failures}')


@app.command()
def start(
    config: str = typer.Option(_DEFAULT_CONFIG, '--config', '-c',
                               help='Path to the YAML config file.'),
) -> None:
    """ Start the continuous daemon and block until SIGINT or SIGTERM.

    Starts three background components:

    - **APScheduler** (interval jobs for every ``histo_job``),
    - **StreamManager** (one thread per ``(exchange, pair)`` WebSocket),
    - **SyncService** (periodic rclone push to all configured remotes).

    A :class:`~dccd.daemon.health.HealthMonitor` is shared across all
    components; metrics and a rotating log file are written to
    ``{local_path}/.dccd/``.

    Press Ctrl-C or send SIGTERM to stop gracefully.

    """
    from dccd.daemon.health import HealthMonitor
    from dccd.daemon.scheduler import build_histo_scheduler
    from dccd.daemon.stream_manager import StreamManager

    cfg = _load(config)
    health = HealthMonitor(cfg.storage.local_path, cfg.alerts)  # type: ignore[attr-defined]
    scheduler = build_histo_scheduler(cfg, health=health)  # type: ignore[arg-type]
    stream_mgr = StreamManager(cfg, health=health)  # type: ignore[arg-type]

    stop_event = threading.Event()

    def _handle_signal(signum: int, frame: object) -> None:
        typer.echo('Stopping daemon…')
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    typer.echo('Starting daemon. Press Ctrl-C to stop.')
    scheduler.start()
    stream_mgr.start()
    stop_event.wait()
    scheduler.shutdown(wait=False)
    stream_mgr.stop()
    typer.echo('Daemon stopped.')


@app.command()
def status(
    config: str = typer.Option(_DEFAULT_CONFIG, '--config', '-c',
                               help='Path to the YAML config file.'),
) -> None:
    """ Print a health table from the saved metrics JSON.

    Reads ``{local_path}/.dccd/metrics.json`` and renders a table with
    one row per ``(exchange, pair)`` job.  Columns: ``job``, ``last_run``,
    ``last_success``, ``rows`` (cumulative), ``errors`` (consecutive).
    Prints ``No metrics yet.`` if the file does not exist.

    """
    cfg = _load(config)
    metrics_file = Path(cfg.storage.local_path) / '.dccd' / 'metrics.json'  # type: ignore[attr-defined]

    if not metrics_file.exists():
        typer.echo('No metrics yet.')
        return

    data: dict = json.loads(metrics_file.read_text())

    def _fmt_ts(ts: float | None) -> str:
        if ts is None:
            return '-'
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')

    col_w = 24
    header = (
        f"{'job':<{col_w}} {'last_run':<17} {'last_success':<17} {'rows':>6} {'errors':>6}"
    )
    typer.echo(header)
    typer.echo('-' * len(header))
    for key, m in data.items():
        typer.echo(
            f"{key:<{col_w}} "
            f"{_fmt_ts(m.get('last_run_at')):<17} "
            f"{_fmt_ts(m.get('last_success_at')):<17} "
            f"{m.get('rows_collected', 0):>6} "
            f"{m.get('errors_count', 0):>6}"
        )


@app.command()
def add(
    exchange: str = typer.Option(..., '--exchange', '-e', help='Exchange name.'),
    pair: str = typer.Option(..., '--pair', '-p', help='Trading pair (e.g. BTC/USDT).'),
    span: int = typer.Option(..., '--span', '-s', help='Candle interval in seconds.'),
    config: str = typer.Option(_DEFAULT_CONFIG, '--config', '-c',
                               help='Path to the YAML config file.'),
) -> None:
    """ Append a new histo job to the YAML config file in-place.

    Adds a ``histo_jobs`` entry for the given ``(exchange, pair, span)``
    and re-validates the whole config with Pydantic before writing.
    Exits with code 1 and leaves the file unchanged if validation fails.

    """
    import yaml
    from pydantic import ValidationError

    from dccd.daemon.config import CollectorConfig

    config_path = Path(config)
    if not config_path.exists():
        typer.echo(f'Error: config file not found: {config}', err=True)
        raise typer.Exit(1)

    raw: dict = yaml.safe_load(config_path.read_text())
    raw.setdefault('histo_jobs', [])
    raw['histo_jobs'].append({
        'exchange': exchange,
        'pairs': [pair],
        'span': span,
    })

    try:
        CollectorConfig.model_validate(raw)
    except ValidationError as exc:
        typer.echo(f'Validation error after add: {exc}', err=True)
        raise typer.Exit(1)

    config_path.write_text(yaml.dump(raw, default_flow_style=False))
    typer.echo(f'Added histo job: exchange={exchange} pair={pair} span={span}s')
    typer.echo(f'Config written to {config}.')
