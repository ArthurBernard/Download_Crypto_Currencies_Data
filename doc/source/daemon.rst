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

Quick start
-----------

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
           span: 3600
           format: parquet
           by_period: Y

       stream_jobs:
         - exchange: binance
           pairs: [BTC/USDT]
           channels: [trades, book]
           time_step: 60

3. Run a one-shot collection, then start the daemon:

   .. code-block:: python

       from dccd.daemon.config import load_config
       from dccd.daemon.scheduler import run_once, build_histo_scheduler
       from dccd.daemon.stream_manager import StreamManager

       config = load_config('config.yml')

       # One-shot: download all histo jobs once and exit
       run_once(config)

       # Daemon: start periodic histo scheduler + WebSocket streams
       scheduler = build_histo_scheduler(config)
       scheduler.start()

       mgr = StreamManager(config)
       mgr.start()

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

Storage
-------

.. autosummary::
   :toctree: generated/

   storage.RemoteStorage -- push local data directories to remote destinations via rclone
