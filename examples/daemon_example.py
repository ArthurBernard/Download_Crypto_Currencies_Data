#!/usr/bin/env python3
# coding: utf-8

"""Programmatic daemon usage — without the CLI.

This script wires the v3 (hexagonal) daemon components together in pure Python.
It is the equivalent of ``dccd start --config config.yml`` but gives you full
control over each component: useful when embedding the daemon inside a larger
process or when you need custom startup/shutdown logic.

The :class:`~dccd.application.scheduler.Scheduler` routes every configured job by
its trigger kind: ``supervised`` → a live WebSocket stream worker, ``interval`` /
``cron`` → a recurring backfill loop, ``once`` → a one-shot backfill.

Components
----------
- :func:`~dccd.application.config.load_config` — parse + validate the YAML config
- :func:`~dccd.application.service_factory.build_store` — local Parquet store
- :func:`~dccd.application.service_factory.build_runs_store` — SQLite run history
- :func:`~dccd.application.service_factory.build_registry` — exchange adapters
- :class:`~dccd.application.events.EventBus` — progress/log/status fan-out
- :class:`~dccd.application.monitor.HealthMonitor` — webhook alerts on failures
- :class:`~dccd.application.scheduler.Scheduler` — orchestrates all jobs

Prerequisites
-------------
    pip install "dccd[daemon]"
    # Copy and adapt examples/config.example.yml to config.yml
    # (or point CONFIG_PATH to any valid YAML config)
"""

import asyncio
import logging

from dccd.application.config import load_config
from dccd.application.events import EventBus
from dccd.application.monitor import HealthMonitor
from dccd.application.scheduler import Scheduler
from dccd.application.service_factory import (
    build_registry,
    build_runs_store,
    build_store,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)

CONFIG_PATH = "config.yml"


async def main() -> None:
    # 1. Load and validate the YAML configuration.
    cfg = load_config(CONFIG_PATH)
    print(f"Loaded config: {len(cfg.jobs)} job(s)")

    # 2. Build the shared services (single source of truth for wiring — same as
    #    the CLI and the HTTP API).
    store = build_store(cfg.settings.data_path)
    runs_store = build_runs_store(cfg.settings.data_path)
    registry = build_registry()
    bus = EventBus()

    # 3. Optional: webhook alerts on repeated failures (config.alerts).
    HealthMonitor(
        runs_store,
        bus,
        webhook_url=cfg.alerts.webhook_url,
        max_consecutive_errors=cfg.alerts.max_consecutive_errors,
    )

    # 4. Start the scheduler with every configured job spec. It keeps running
    #    until cancelled (Ctrl-C), reconnecting streams and re-arming backfills.
    scheduler = Scheduler(registry, store, runs_store, bus)
    print("Starting daemon. Press Ctrl-C to stop.")
    await scheduler.start(cfg.all_job_specs())
    try:
        await asyncio.Event().wait()  # block forever
    finally:
        await scheduler.stop()
        print("Daemon stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutting down daemon…")
