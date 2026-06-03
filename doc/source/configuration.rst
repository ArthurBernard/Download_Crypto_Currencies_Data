=======================
Configuration Reference
=======================

The daemon and CLI are driven by a YAML config validated by Pydantic
(:class:`dccd.application.config.AppConfig`). Validate it with:

.. code-block:: bash

   dccd validate --config config.yml

Example
=======

.. code-block:: yaml

   settings:
     data_path: /home/me/data/crypto
     timezone: UTC
     ui_host: 127.0.0.1
     ui_port: 8080
     ui_auth_token: null        # set a token to require Bearer auth on /api/*
     ui_allow_origins: []        # opt-in CORS origins (default: same-origin only)

   storage:
     local_path: /home/me/data/crypto
     remotes: []                 # rclone remotes for sync
     sync_interval: 0

   alerts:
     webhook_url: null
     max_consecutive_errors: 3

   jobs:
     - exchange: binance
       pairs: [BTC/USDT, ETH/USDT]
       data_type: ohlc
       operation: backfill
       span: 60
       trigger_kind: interval
       every: 60
       start: last

     - exchange: binance
       pairs: [BTC/USDT]
       data_type: trades
       operation: stream
       trigger_kind: supervised
       start: last

Sections
========

``settings``
   ``data_path`` (root for Parquet files), ``timezone`` (``local``/``UTC``/IANA),
   web UI ``ui_host`` / ``ui_port``, optional ``ui_auth_token`` (enables Bearer
   auth on ``/api/*``) and ``ui_allow_origins`` (opt-in CORS).

``storage``
   ``local_path``, rclone ``remotes``, and ``sync_interval`` (seconds; ``0``
   disables periodic sync).

``alerts``
   ``webhook_url`` and ``max_consecutive_errors`` for health alerts.

``jobs``
   A list of job definitions. Each expands over ``pairs`` into one job spec per
   pair. Key fields: ``exchange``, ``data_type`` (``ohlc``/``trades``/
   ``orderbook``), ``operation`` (``backfill``/``stream``), ``span`` (required
   for OHLC), ``trigger_kind`` (``interval``/``cron``/``supervised``/``once``),
   ``every`` / ``cron`` for scheduling, ``start``, ``depth`` and
   ``snapshot_interval`` (order book).
