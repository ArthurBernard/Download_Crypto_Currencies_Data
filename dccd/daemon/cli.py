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

    dccd remove --exchange X --pair Y --span N [--config PATH]
        Remove a pair from a histo_job (or the whole job if it was the last
        pair) and re-validate before writing.

    dccd inventory [--config PATH]
        Scan data_path and print a table of all stored data (OHLC, trades,
        orderbook) with date range, row count, and gap count per series.

"""

from __future__ import annotations

import json
import signal
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List, Optional

import typer

__all__ = ['app']

app = typer.Typer(help='dccd — autonomous crypto data collection daemon')


def _complete_histo_exchange(incomplete: str) -> list[str]:
    from dccd.daemon.config import SUPPORTED_HISTO_EXCHANGES
    return [e for e in sorted(SUPPORTED_HISTO_EXCHANGES) if e.startswith(incomplete)]


def _complete_pairs_from_config(incomplete: str) -> list[str]:
    from dccd.daemon.config import load_config, resolve_config_path
    try:
        cfg = load_config(resolve_config_path(None))
        pairs: set[str] = set()
        for job in cfg.histo_jobs:  # type: ignore[attr-defined]
            pairs.update(job.pairs)
        return [p for p in sorted(pairs) if p.startswith(incomplete)]
    except Exception:
        return []


_LOOPBACK_HOSTS = frozenset({'127.0.0.1', 'localhost', '::1'})


def _warn_open_bind(host: str, token: object) -> None:
    """ Warn when the UI binds to a non-loopback address without a token.

    Exposing the UI on the network without ``ui_auth_token`` lets anyone
    edit the config, launch backfills, and trigger syncs unauthenticated.
    """
    if host not in _LOOPBACK_HOSTS and not token:
        typer.echo(
            f'WARNING: web UI bound to {host} without ui_auth_token — '
            'anyone on the network can control the daemon. '
            'Set settings.ui_auth_token or bind to 127.0.0.1.',
            err=True,
        )


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
        autocompletion=_complete_histo_exchange,
    ),
    pairs: Optional[List[str]] = typer.Option(
        None, '--pairs', '-p',
        help='Restrict to specific pairs, e.g. --pairs BTC/USDT ETH/USDT.',
        autocompletion=_complete_pairs_from_config,
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
    from dccd.daemon.config import resolve_config_path
    from dccd.daemon.health import HealthMonitor
    from dccd.daemon.scheduler import build_histo_scheduler
    from dccd.daemon.stream_manager import StreamManager

    config_path = resolve_config_path(config)
    cfg = _load(str(config_path))
    health = HealthMonitor(cfg.storage.local_path, cfg.alerts)  # type: ignore[attr-defined]
    scheduler = build_histo_scheduler(cfg, health=health)  # type: ignore[arg-type]
    stream_mgr = StreamManager(cfg, health=health)  # type: ignore[arg-type]

    ui_server = _start_ui_thread(config_path, cfg)

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
    if ui_server is not None:
        ui_server.should_exit = True
    typer.echo('Daemon stopped.')


def _start_ui_thread(config_path: Path, cfg: object) -> Any | None:
    """ Start the web UI in a background thread, or ``None`` if unavailable.

    Returns the ``uvicorn.Server`` instance (so the caller can signal it to
    exit on shutdown), or ``None`` when the ``[ui]`` extra is not installed.
    Signal handlers are disabled because the server runs off the main thread.
    """
    try:
        import uvicorn

        from dccd.daemon.api import create_app
    except ImportError:
        typer.echo('Web UI not available (install dccd[ui]). Continuing without it.')
        return None

    host = cfg.settings.ui_host  # type: ignore[attr-defined]
    port = cfg.settings.ui_port  # type: ignore[attr-defined]
    _warn_open_bind(host, cfg.settings.ui_auth_token)  # type: ignore[attr-defined]
    server = uvicorn.Server(uvicorn.Config(
        create_app(config_path), host=host, port=port, log_level='warning',
    ))
    server.install_signal_handlers = False
    threading.Thread(target=server.run, daemon=True, name='dccd-ui').start()
    typer.echo(f'Web UI on http://{host}:{port}')
    return server


@app.command()
def status(
    config: Optional[str] = typer.Option(
        None, '--config', '-c',
        help='Path to the YAML config file (default: ./config.yml or ~/.config/dccd/config.yml).',
    ),
    json_out: bool = typer.Option(False, '--json', help='Output raw metrics as JSON on stdout.'),
) -> None:
    """ Print a health table from the saved metrics JSON.

    Reads ``{local_path}/.dccd/metrics.json`` and renders a table with
    one row per ``(exchange, pair)`` job.  Columns: ``job``, ``last_run``,
    ``last_success``, ``rows`` (cumulative), ``errors`` (consecutive).
    Prints ``No metrics yet.`` if the file does not exist.

    Pass ``--json`` to emit the raw metrics dict as JSON on stdout instead,
    suitable for piping into Grafana, jq, or other tooling.

    """
    cfg = _load(config)
    metrics_file = Path(cfg.storage.local_path) / '.dccd' / 'metrics.json'  # type: ignore[attr-defined]

    if not metrics_file.exists():
        typer.echo('{}' if json_out else 'No metrics yet.')
        return

    data: dict = json.loads(metrics_file.read_text())

    if json_out:
        typer.echo(json.dumps(data, indent=2))
        return

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
    exchange: str = typer.Option(..., '--exchange', '-e', help='Exchange name.',
                                 autocompletion=_complete_histo_exchange),
    pair: str = typer.Option(..., '--pair', '-p', help='Trading pair (e.g. BTC/USDT).',
                             autocompletion=_complete_pairs_from_config),
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


@app.command()
def remove(
    exchange: str = typer.Option(..., '--exchange', '-e', help='Exchange name.',
                                 autocompletion=_complete_histo_exchange),
    pair: str = typer.Option(..., '--pair', '-p', help='Trading pair (e.g. BTC/USDT).',
                             autocompletion=_complete_pairs_from_config),
    span: int = typer.Option(..., '--span', '-s', help='Candle interval in seconds.'),
    config: Optional[str] = typer.Option(
        None, '--config', '-c',
        help='Path to the YAML config file (default: ./config.yml or ~/.config/dccd/config.yml).',
    ),
) -> None:
    """ Remove a pair from a histo_job in the YAML config file in-place.

    Finds the ``histo_job`` matching ``(exchange, span)`` that contains
    *pair*, removes *pair* from its ``pairs`` list, and removes the whole
    job if the list becomes empty.  Re-validates the config with Pydantic
    before writing; exits with code 1 and leaves the file unchanged if
    validation fails (e.g. removing the last job).

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
    jobs: list = raw.get('histo_jobs', [])

    target = next(
        (j for j in jobs if j.get('exchange') == exchange
         and j.get('span') == span and pair in j.get('pairs', [])),
        None,
    )
    if target is None:
        typer.echo(
            f'No matching job found: exchange={exchange} pair={pair} span={span}s',
            err=True,
        )
        raise typer.Exit(1)

    target['pairs'].remove(pair)
    if not target['pairs']:
        jobs.remove(target)

    try:
        CollectorConfig.model_validate(raw)
    except ValidationError as exc:
        typer.echo(f'Validation error after remove: {exc}', err=True)
        raise typer.Exit(1)

    config_path.write_text(yaml.dump(raw, default_flow_style=False))
    typer.echo(f'Removed {pair} from {exchange}/{span}s.')
    typer.echo(f'Config written to {config_path}.')


@app.command()
def ui(
    config: Optional[str] = typer.Option(
        None, '--config', '-c',
        help='Path to the YAML config file (default: ./config.yml or ~/.config/dccd/config.yml).',
    ),
    host: Optional[str] = typer.Option(
        None, '--host', help='Bind address (overrides settings.ui_host).',
    ),
    port: Optional[int] = typer.Option(
        None, '--port', help='TCP port (overrides settings.ui_port).',
    ),
) -> None:
    """ Serve the web UI standalone (no collection, monitoring only).

    Starts a uvicorn server hosting the FastAPI app from
    :func:`~dccd.daemon.api.create_app`.  The bind address and port default
    to ``settings.ui_host`` / ``settings.ui_port`` from the config and can be
    overridden with ``--host`` / ``--port``.

    The UI reads and writes the same config file and data directory as the
    daemon, so it can run alongside ``dccd start`` (or be embedded in it).

    """
    import uvicorn

    from dccd.daemon.api import create_app
    from dccd.daemon.config import resolve_config_path

    try:
        config_path = resolve_config_path(config)
    except FileNotFoundError as exc:
        typer.echo(f'Error: {exc}', err=True)
        raise typer.Exit(1)

    cfg = _load(str(config_path))
    h = host or cfg.settings.ui_host  # type: ignore[attr-defined]
    p = port or cfg.settings.ui_port  # type: ignore[attr-defined]
    _warn_open_bind(h, cfg.settings.ui_auth_token)  # type: ignore[attr-defined]
    typer.echo(f'dccd UI on http://{h}:{p} (config: {config_path})')
    uvicorn.run(create_app(config_path), host=h, port=p)


@app.command()
def inventory(
    config: Optional[str] = typer.Option(
        None, '--config', '-c',
        help='Path to the YAML config file (default: ./config.yml or ~/.config/dccd/config.yml).',
    ),
) -> None:
    """ Print a table of all data stored under data_path.

    Scans ``{data_path}/{exchange}/{ohlc|trades|orderbook}/…`` for Parquet
    files and reports, for each series: exchange, pair, data type, span
    (OHLC only), date range, row count, and gap count (missing years for
    OHLC, missing days for trades/orderbook).

    Requires ``polars`` (included in the ``daemon`` and ``io`` extras).

    """
    import collections
    from datetime import date
    from pathlib import Path as _Path

    import polars as pl

    from dccd.tools.date_time import TS_to_date

    cfg = _load(config)
    data_path = _Path(cfg.storage.local_path)  # type: ignore[attr-defined]

    _DATA_TYPES = ('ohlc', 'trades', 'orderbook')
    groups: dict[tuple[str, str, str, str], list[_Path]] = collections.defaultdict(list)

    for dt in _DATA_TYPES:
        for f in data_path.glob(f'*/{dt}/**/*.parquet'):
            parts = f.relative_to(data_path).parts
            # ohlc:  (exchange, 'ohlc', pair_slug, span_label, 'YYYY.parquet')     len=5
            # other: (exchange, 'trades'|'orderbook', pair_slug, 'YYYY-MM-DD.parquet') len=4
            exchange_name = parts[0]
            pair_slug = parts[2]
            span_lbl = parts[3] if dt == 'ohlc' and len(parts) == 5 else '-'
            groups[(exchange_name, dt, pair_slug, span_lbl)].append(f)

    if not groups:
        typer.echo(f'No data found under {data_path}.')
        return

    rows = []
    for (exchange_name, dt, pair_slug, span_lbl), files in sorted(groups.items()):
        files = sorted(files)
        ts_col = pl.concat([
            pl.read_parquet(f, columns=['TS']) for f in files
        ])['TS']
        total_rows = len(ts_col)
        ts_min = ts_col.min()
        ts_max = ts_col.max()
        from_date = TS_to_date(int(ts_min), form='%Y-%m-%d') if ts_min is not None else '-'
        to_date = TS_to_date(int(ts_max), form='%Y-%m-%d') if ts_max is not None else '-'

        if dt == 'ohlc':
            existing_years = {int(f.stem) for f in files}
            gaps = len(set(range(min(existing_years), max(existing_years) + 1)) - existing_years)
        else:
            existing_days = {f.stem for f in files}
            d0 = date.fromisoformat(min(existing_days))
            d1 = date.fromisoformat(max(existing_days))
            gaps = (d1 - d0).days + 1 - len(existing_days)

        pair = pair_slug.replace('-', '/', 1)
        rows.append((exchange_name, pair, dt, span_lbl, from_date, to_date, total_rows, gaps))

    ew, pw, tw, sw, fw, tow, rw, gw = 10, 12, 11, 6, 12, 12, 8, 6
    header = (
        f"{'exchange':<{ew}} {'pair':<{pw}} {'type':<{tw}} {'span':<{sw}} "
        f"{'from':<{fw}} {'to':<{tow}} {'rows':>{rw}} {'gaps':>{gw}}"
    )
    typer.echo(header)
    typer.echo('-' * len(header))
    for exchange_name, pair, dt, span_lbl, from_date, to_date, total_rows, gaps in rows:
        typer.echo(
            f"{exchange_name:<{ew}} {pair:<{pw}} {dt:<{tw}} {span_lbl:<{sw}} "
            f"{from_date:<{fw}} {to_date:<{tow}} {total_rows:>{rw},} {gaps:>{gw}}"
        )
