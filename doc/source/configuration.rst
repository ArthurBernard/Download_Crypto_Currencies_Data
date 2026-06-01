========================
Configuration Reference
========================

The ``dccd`` daemon is driven by a YAML config file.  The full annotated
template is at ``examples/config.example.yml`` in the repository.

.. code-block:: bash

   # Generate a starter config
   cp examples/config.example.yml config.yml
   dccd validate --config config.yml

----

Top-level structure
-------------------

.. code-block:: yaml

   settings:
     data_path: /data/crypto
     log_level: INFO

   storage:
     local_path: /data/crypto
     remotes: []           # optional rclone destinations

   histo_jobs:
     - exchange: binance
       pairs: [BTC/USDT]
       span: 3600

   stream_jobs:
     - exchange: binance
       pairs: [BTC/USDT]
       channels: [trades, book]
       time_step: 60

   alerts:
     webhook_url: https://hooks.slack.com/...
     max_consecutive_errors: 5

----

``settings`` block
------------------

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Field
     - Default
     - Description
   * - ``data_path``
     - ``./data/crypto``
     - Root directory where all data files are stored.
   * - ``timezone``
     - ``local``
     - ``local``, ``UTC``, or an IANA name (e.g. ``Europe/Paris``).
   * - ``ui_host``
     - ``127.0.0.1``
     - Bind address for the web UI (``dccd ui``).  Use ``0.0.0.0`` to expose
       it on the network.
   * - ``ui_port``
     - ``8080``
     - TCP port for the web UI.
   * - ``ui_auth_token``
     - ``null``
     - Bearer token required to access the web UI.  ``null`` disables auth
       (appropriate only for a local ``127.0.0.1`` bind).

----

``storage`` block
-----------------

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Field
     - Default
     - Description
   * - ``local_path``
     - *(required)*
     - Same as ``settings.data_path`` — root for Parquet files.
   * - ``remotes``
     - ``[]``
     - List of rclone remote destinations (see below).

rclone remotes
~~~~~~~~~~~~~~

.. code-block:: yaml

   storage:
     local_path: /data/crypto
     remotes:
       - name: s3-backup
         path: s3:my-bucket/crypto
         sync_interval: 3600   # seconds between rclone syncs

----

``histo_jobs`` block
--------------------

Each entry schedules periodic OHLCV REST downloads for one exchange and span.

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Field
     - Default
     - Description
   * - ``exchange``
     - *(required)*
     - Exchange name: ``binance``, ``kraken``, ``bybit``, ``okx``, ``coinbase``.
   * - ``pairs``
     - *(required)*
     - List of pairs, e.g. ``[BTC/USDT, ETH/USDT]``.
   * - ``span``
     - *(required)*
     - Candle interval in seconds: 60, 300, 900, 3600, 14400, 86400.
   * - ``start``
     - ``'2017-01-01'``
     - Earliest date to backfill from.
   * - ``max_retries``
     - ``3``
     - Number of retry attempts on transient network errors (1–10).
   * - ``retry_delay``
     - ``2.0``
     - Base delay in seconds; actual delay is ``retry_delay × 2^(attempt-1)``.

Example with retry configuration:

.. code-block:: yaml

   histo_jobs:
     - exchange: okx
       pairs: [BTC/USDT, ETH/USDT, SOL/USDT]
       span: 3600
       start: '2022-01-01'
       max_retries: 5
       retry_delay: 3.0

----

``stream_jobs`` block
---------------------

Each entry opens a persistent WebSocket stream for one exchange.

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Field
     - Default
     - Description
   * - ``exchange``
     - *(required)*
     - Exchange name: ``binance``, ``kraken``, ``bybit``, ``okx``, ``bitfinex``, ``bitmex``.
   * - ``pairs``
     - *(required)*
     - List of pairs.
   * - ``channels``
     - *(required)*
     - List of channels: ``trades``, ``book``.  Kraken also supports ``ohlcv``.
   * - ``time_step``
     - ``60``
     - Snapshot interval in seconds — how often data is flushed to disk.
   * - ``until``
     - ``None``
     - Total run duration in seconds.  ``None`` means run indefinitely.

.. code-block:: yaml

   stream_jobs:
     - exchange: kraken
       pairs: [BTC/USD, ETH/USD]
       channels: [trades, book, ohlcv]
       time_step: 60

----

``alerts`` block
----------------

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Field
     - Default
     - Description
   * - ``webhook_url``
     - ``None``
     - Slack (or any webhook) URL; a POST is sent when the error threshold is hit.
   * - ``max_consecutive_errors``
     - ``5``
     - Number of consecutive job errors before an alert is fired.

Slack example:

.. code-block:: yaml

   alerts:
     webhook_url: https://hooks.slack.com/services/T.../B.../xxx
     max_consecutive_errors: 3

The webhook payload is a JSON object ``{"text": "dccd alert: <message>"}``.

----

Complete example
----------------

.. code-block:: yaml

   settings:
     data_path: /data/crypto
     log_level: INFO

   storage:
     local_path: /data/crypto
     remotes:
       - name: s3
         path: s3:my-bucket/crypto
         sync_interval: 3600

   histo_jobs:
     - exchange: binance
       pairs: [BTC/USDT, ETH/USDT]
       span: 3600
       max_retries: 3
       retry_delay: 2.0

     - exchange: kraken
       pairs: [BTC/USD]
       span: 3600

   stream_jobs:
     - exchange: binance
       pairs: [BTC/USDT]
       channels: [trades, book]
       time_step: 60

   alerts:
     webhook_url: https://hooks.slack.com/services/T.../B.../xxx
     max_consecutive_errors: 5

----

Config models reference
-----------------------

.. autosummary::
   :toctree: generated/

   dccd.daemon.config.CollectorConfig
   dccd.daemon.config.SettingsConfig
   dccd.daemon.config.StorageConfig
   dccd.daemon.config.RemoteConfig
   dccd.daemon.config.HistoJob
   dccd.daemon.config.StreamJob
   dccd.daemon.config.AlertConfig
