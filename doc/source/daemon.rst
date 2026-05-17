-----------------------------------------
 Daemon (:mod:`dccd.daemon`)
-----------------------------------------

.. automodule:: dccd.daemon
   :no-members:
   :no-inherited-members:
   :no-special-members:

The daemon module provides an autonomous, server-side data collector.
It reads a declarative YAML configuration, runs historical REST jobs on a
schedule (APScheduler), opens WebSocket streams for real-time collection, and
periodically syncs all local data to one or more remote destinations via rclone.
Per-job metrics and a rotating log file are maintained by
:class:`~dccd.daemon.health.HealthMonitor`.

Quick start (CLI)
-----------------

1. Install the daemon extra:

   .. code-block:: bash

       pip install "dccd[daemon]"

2. Write a configuration file (see :ref:`daemon-config`):

   .. code-block:: yaml

       storage:
         local_path: /data/crypto/
         remotes:
           - provider: rclone
             remote: "mynas:crypto/"
         sync_interval: 3600

       histo_jobs:
         - exchange: binance
           pairs: [BTC/USDT, ETH/USDT]
           span: 3600          # candle interval in seconds
           format: parquet
           by_period: Y        # one file per year

       # Optional real-time streams
       stream_jobs:
         - exchange: binance
           pairs: [BTC/USDT]
           channels: [trades, book]
           time_step: 60

       # Optional webhook alerts on consecutive failures
       alerts:
         webhook_url: "https://hooks.slack.com/services/..."
         max_consecutive_errors: 3

3. Validate, run once, then start the daemon:

   .. code-block:: bash

       # Check config without running anything
       dccd validate --config config.yml
       # Config: config.yml
       #   storage.local_path : /data/crypto/
       #   remotes            : 1
       #   histo_jobs         : 1
       #   stream_jobs        : 1
       # Config is valid.

       # One-shot: download all histo jobs once and exit
       dccd run --config config.yml
       # Done. successes=2 failures=0

       # Continuous daemon (block until Ctrl-C / SIGTERM)
       dccd start --config config.yml

       # Inspect per-job health after the daemon has run
       dccd status --config config.yml
       # job                      last_run          last_success       rows  errors
       # -------------------------------------------------------------------------
       # binance/BTC/USDT         2026-05-17 10:00  2026-05-17 10:00   1200       0
       # binance/ETH/USDT         2026-05-17 10:00  2026-05-17 10:00    980       0

       # Add a new histo job to an existing config in-place
       dccd add --exchange kraken --pair ETH/USD --span 86400 --config config.yml

Python API
----------

Use the components directly when you need to embed the daemon inside your
own process or customise startup/shutdown logic.  The script
:file:`examples/daemon_example.py` shows the full wiring:

.. code-block:: python

    from dccd.daemon.config import load_config
    from dccd.daemon.health import HealthMonitor
    from dccd.daemon.scheduler import build_histo_scheduler, run_once
    from dccd.daemon.stream_manager import StreamManager

    config  = load_config('config.yml')
    health  = HealthMonitor(config.storage.local_path, config.alerts)

    # --- one-shot mode (cron-friendly) ---
    run_once(config, health=health)

    # --- or continuous mode ---
    scheduler  = build_histo_scheduler(config, health=health)
    stream_mgr = StreamManager(config, health=health)
    scheduler.start()
    stream_mgr.start()
    # … wait for stop signal …
    scheduler.shutdown(wait=False)
    stream_mgr.stop()

.. _daemon-config:

Configuration
-------------

.. autosummary::
   :toctree: generated/

   config.load_config -- load and validate a YAML configuration file
   config.CollectorConfig -- root configuration model
   config.StorageConfig -- local storage path and remote sync settings
   config.RemoteConfig -- one rclone remote destination
   config.HistoJob -- historical (REST) data collection job
   config.StreamJob -- real-time (WebSocket) data collection job
   config.AlertConfig -- optional webhook alerting settings

Scheduler
---------

.. autosummary::
   :toctree: generated/

   scheduler.build_histo_scheduler -- build an APScheduler BackgroundScheduler from config
   scheduler.run_histo_job -- download and save one (exchange, pair) candle job
   scheduler.run_once -- execute all histo_jobs once and return

Stream manager
--------------

.. autosummary::
   :toctree: generated/

   stream_manager.StreamManager -- manage real-time WebSocket collection jobs
   stream_manager.SyncService -- periodically push local data to all remote destinations

Health monitoring
-----------------

.. autosummary::
   :toctree: generated/

   health.HealthMonitor -- track per-job metrics, write rotating logs, send webhook alerts
   health.JobMetrics -- per-job health metrics dataclass

Storage
-------

.. autosummary::
   :toctree: generated/

   storage.RemoteStorage -- push local data directories to remote destinations via rclone

CLI
---

The ``dccd`` command is a `typer <https://typer.tiangolo.com/>`_ application
installed as a console script by ``pip install "dccd[daemon]"``.
Its commands are documented in the *Quick start* section above.

.. automodule:: dccd.daemon.cli
   :no-members:
   :no-inherited-members:
   :no-special-members:
