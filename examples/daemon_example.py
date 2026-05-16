#!/usr/bin/env python3
# coding: utf-8

""" Programmatic daemon usage — without the CLI.

This script shows how to wire the daemon components together in pure Python.
It is the equivalent of ``dccd start --config config.yml`` but gives you full
control over each component: useful when embedding the daemon inside a larger
process or when you need custom startup/shutdown logic.

Components
----------
- :func:`~dccd.daemon.config.load_config` — parse and validate the YAML config
- :class:`~dccd.daemon.health.HealthMonitor` — rotating log + metrics JSON
- :func:`~dccd.daemon.scheduler.build_histo_scheduler` — APScheduler for REST jobs
- :class:`~dccd.daemon.stream_manager.StreamManager` — WebSocket threads
- :func:`~dccd.daemon.scheduler.run_once` — one-shot alternative to the scheduler

Prerequisites
-------------
    pip install "dccd[daemon]"
    # Copy and adapt examples/config.example.yml to config.yml
    # (or point CONFIG_PATH to any valid YAML config)

"""

import logging
import signal
import threading

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(name)s %(levelname)s %(message)s',
)

CONFIG_PATH = 'config.yml'

# ---------------------------------------------------------------------------
# 1. Load and validate the YAML configuration
# ---------------------------------------------------------------------------
from dccd.daemon.config import load_config

config = load_config(CONFIG_PATH)
print(f'Loaded config: {len(config.histo_jobs)} histo jobs, '
      f'{len(config.stream_jobs)} stream jobs')

# ---------------------------------------------------------------------------
# 2. Initialise the health monitor
#
#    Creates {local_path}/.dccd/ with:
#      - dccd.log  (rotating, 10 MB × 5 files)
#      - metrics.json (updated after every job execution)
#    Optional webhook alerts are configured in config.alerts.
# ---------------------------------------------------------------------------
from dccd.daemon.health import HealthMonitor

health = HealthMonitor(config.storage.local_path, config.alerts)

# ---------------------------------------------------------------------------
# 3a. One-shot run: execute every histo_job once, then return
#
#     Use this in a cron job or for an initial backfill.
#     Failed jobs are logged and skipped; others continue.
# ---------------------------------------------------------------------------
from dccd.daemon.scheduler import run_once

# run_once(config, health=health)   # uncomment for one-shot mode

# ---------------------------------------------------------------------------
# 3b. Continuous daemon: scheduler + stream manager
#
#     build_histo_scheduler() creates an APScheduler BackgroundScheduler
#     with one interval job per (exchange, pair) defined in histo_jobs.
#
#     StreamManager starts one background thread per (exchange, pair) for
#     every stream_job; threads restart automatically on crash.
# ---------------------------------------------------------------------------
from dccd.daemon.scheduler import build_histo_scheduler
from dccd.daemon.stream_manager import StreamManager

scheduler = build_histo_scheduler(config, health=health)
stream_mgr = StreamManager(config, health=health)

# ---------------------------------------------------------------------------
# 4. Graceful shutdown on SIGINT / SIGTERM
# ---------------------------------------------------------------------------
stop_event = threading.Event()


def _shutdown(signum, frame):
    print('\nShutting down daemon…')
    stop_event.set()


signal.signal(signal.SIGINT, _shutdown)
signal.signal(signal.SIGTERM, _shutdown)

# ---------------------------------------------------------------------------
# 5. Start and block
# ---------------------------------------------------------------------------
print('Starting daemon. Press Ctrl-C to stop.')
scheduler.start()
stream_mgr.start()

stop_event.wait()

scheduler.shutdown(wait=False)
stream_mgr.stop()

# ---------------------------------------------------------------------------
# 6. Print a final metrics snapshot
# ---------------------------------------------------------------------------
metrics = health.get_metrics()
print(f'\nFinal metrics ({len(metrics)} jobs):')
for key, m in metrics.items():
    print(f'  {key:30s}  rows={m.rows_collected:6d}  errors={m.errors_count}')
