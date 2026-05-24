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

    dccd backfill --config PATH [--exchange X] [--pairs A B] [--start DATE]
                  [--parallel] [--dry-run]
        Download the full OHLC history for every histo_job defined in the
        config, resuming from the last saved timestamp.  Runs in the
        foreground; use --parallel to run all jobs simultaneously.

    dccd collect --config PATH
        Fetch one incremental batch per histo_job, then exit.
        Downloads candles from the last saved timestamp to now.
        Designed for cron scheduling or as a single daemon tick.

    dccd start --config PATH
        Start the continuous daemon in the foreground:
        - APScheduler BackgroundScheduler for all histo_jobs (calls collect)
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
from typing import List, Optional

import typer

__all__ = ['app']

app = typer.Typer(help='dccd — autonomous crypto data collection daemon')

def _load(config_path: Optional[str]) -> object:
    """Load config, exit with code 1 on any error."""
    from dccd.daemon.config import load_config, resolve_config_path

    try:
        resolved = resolve_config_path(config_path)
        return load_config(resolved)
    except FileNotFoundError as exc:
        typer.echo(f'Error: {exc}', err=True)
        raise typer.Exit(1)
    except Exception as exc:
        typer.echo(f'Error: {exc}', err=True)
        raise typer.Exit(1)


@app.command()
def validate(
    config: Optional[str] = typer.Option(
        None, '--config', '-c',
        help='Path to the YAML config file (default: ./config.yml or ~/.config/dccd/config.yml).',
    ),
) -> None:
    """ Validate a YAML config file and print a one-line summary.

    Loads the file, runs Pydantic validation, and prints a count of
    histo_jobs, stream_jobs, remotes, and the local storage path.
    Exits with code 1 on any error (missing file, bad YAML, invalid config).

    """
    from dccd.daemon.config import resolve_config_path

    try:
        resolved = resolve_config_path(config)
    except FileNotFoundError as exc:
        typer.echo(f'Error: {exc}', err=True)
        raise typer.Exit(1)
    cfg = _load(str(resolved))
    typer.echo(f'Config: {resolved}')
    typer.echo(f'  data_path          : {cfg.settings.data_path}')  # type: ignore[attr-defined]
    typer.echo(f'  timezone           : {cfg.settings.timezone}')  # type: ignore[attr-defined]
    typer.echo(f'  remotes            : {len(cfg.storage.remotes)}')  # type: ignore[attr-defined]
    typer.echo(f'  histo_jobs         : {len(cfg.histo_jobs)}')  # type: ignore[attr-defined]
    typer.echo(f'  stream_jobs        : {len(cfg.stream_jobs)}')  # type: ignore[attr-defined]
    typer.echo('Config is valid.')


@app.command()
def backfill(
    config: Optional[str] = typer.Option(
        None, '--config', '-c',
        help='Path to the YAML config file (default: ./config.yml or ~/.config/dccd/config.yml).',
    ),
    exchange: Optional[str] = typer.Option(
        None, '--exchange', '-e',
        help='Restrict to one exchange (e.g. binance, kraken, bybit).',
    ),
    pairs: Optional[List[str]] = typer.Option(
        None, '--pairs', '-p',
        help='Restrict to specific pairs, e.g. --pairs BTC/USDT ETH/USDT.',
    ),
    start: str = typer.Option(
        '2020-01-01 00:00:00', '--start',
        help='Earliest date to backfill (YYYY-MM-DD HH:MM:SS).',
    ),
    parallel: bool = typer.Option(
        False, '--parallel', help='Run all jobs in parallel threads.',
    ),
    dry_run: bool = typer.Option(
        False, '--dry-run', help='Estimate windows and time without downloading.',
    ),
) -> None:
    """ Download the full OHLC history for all histo_jobs in the config.

    Reads exchanges, pairs, span, and format from the config file.
    Resumes from the last saved timestamp for each pair so the command is
    safe to run repeatedly.

    Kraken uses a trades-based strategy (Kraken's OHLC endpoint does not
    support arbitrary historical windows); all other exchanges use the
    standard OHLC endpoint.

    """
    from dccd.daemon.backfill import run_backfill

    cfg = _load(config)
    run_backfill(cfg, exchange=exchange, pairs=list(pairs) if pairs else None,  # type: ignore[arg-type]
                 start=start, parallel=parallel, dry_run=dry_run)


@app.command()
def collect(
    config: Optional[str] = typer.Option(
        None, '--config', '-c',
        help='Path to the YAML config file (default: ./config.yml or ~/.config/dccd/config.yml).',
    ),
) -> None:
    """ Fetch one incremental batch per histo_job, then exit.

    Downloads candles from the last saved timestamp to now for each
    ``(exchange, pair)`` in ``histo_jobs``.  Intended for cron-based
    scheduling or as a single tick of the continuous daemon
    (``dccd start`` calls this logic in a loop).

    A :class:`~dccd.daemon.health.HealthMonitor` is instantiated so
    metrics are persisted even for this one-shot run.  Failed jobs are
    logged and skipped; remaining jobs continue.
    Prints ``successes=N failures=M`` on completion.

    See Also
    --------
    backfill : full historical download with gap detection.
    start    : continuous daemon that calls collect in a loop.

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
    config: Optional[str] = typer.Option(
        None, '--config', '-c',
        help='Path to the YAML config file (default: ./config.yml or ~/.config/dccd/config.yml).',
    ),
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
    config: Optional[str] = typer.Option(
        None, '--config', '-c',
        help='Path to the YAML config file (default: ./config.yml or ~/.config/dccd/config.yml).',
    ),
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
    config: Optional[str] = typer.Option(
        None, '--config', '-c',
        help='Path to the YAML config file (default: ./config.yml or ~/.config/dccd/config.yml).',
    ),
) -> None:
    """ Append a new histo job to the YAML config file in-place.

    Adds a ``histo_jobs`` entry for the given ``(exchange, pair, span)``
    and re-validates the whole config with Pydantic before writing.
    Exits with code 1 and leaves the file unchanged if validation fails.

    """
    import yaml
    from pydantic import ValidationError

    from dccd.daemon.config import CollectorConfig, resolve_config_path

    try:
        config_path = resolve_config_path(config)
    except FileNotFoundError as exc:
        typer.echo(f'Error: {exc}', err=True)
        raise typer.Exit(1)

    if not config_path.exists():
        typer.echo(f'Error: config file not found: {config_path}', err=True)
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
    typer.echo(f'Config written to {config_path}.')
