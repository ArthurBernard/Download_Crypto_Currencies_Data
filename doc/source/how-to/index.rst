=============
How-to guides
=============

Task-oriented recipes — short answers to "how do I …?". They assume you've done
the :doc:`tutorials </tutorials/index>` and know the basics.

Schedule daily collection
=========================

Declare interval/stream jobs in ``config.yml`` and run the daemon; it schedules
backfills and supervises streams (auto-reconnect):

.. code-block:: yaml

   jobs:
     - {exchange: binance, pairs: [BTC/USDT, ETH/USDT], data_type: ohlc,
        operation: backfill, span: 3600, trigger_kind: interval, every: 3600, start: last}
     - {exchange: binance, pairs: [BTC/USDT], data_type: trades,
        operation: stream, trigger_kind: supervised, start: last}

.. code-block:: bash

   dccd start

Backfill deep trade history (and cancel a runaway)
==================================================

Trades are cursor-paginated and drain the **whole** window — a day of a liquid
pair is millions of rows. Give an explicit ``start`` date:

.. code-block:: bash

   dccd backfill -e okx -s BTC/USDT -t trades --start 2024-01-01

From the web UI, the backfill modal shows live progress and a **Stop** button
(it keeps everything already collected). Via the API:

.. code-block:: bash

   curl -XDELETE localhost:8080/api/backfill/$RUN_ID

Analyse stored data in Polars or Pandas
=======================================

:meth:`~dccd.Client.read` returns a Polars DataFrame; convert if you prefer
Pandas:

.. code-block:: python

   df = c.read("binance", "BTC/USDT", "ohlc", span=3600)
   pdf = df.to_pandas()

Or read the Parquet files directly with any tool —
``{data_path}/{exchange}/ohlc/{pair}/{span}/{year}.parquet``.

Sync data to a remote (S3, GCS, …)
==================================

Configure an `rclone <https://rclone.org/>`_ remote and a sync interval; the
daemon pushes after each cycle:

.. code-block:: yaml

   storage:
     remotes:
       - {provider: rclone, remote: "s3:my-bucket/crypto"}
     sync_interval: 3600

Migrate data from dccd v2
=========================

Upgrade legacy Parquet files to the v3 schema (back up first):

.. code-block:: bash

   dccd migrate --dry-run       # preview
   dccd migrate --no-dry-run    # apply (idempotent, never drops rows)

Protect the web UI with a token
===============================

Set a token; the API then requires ``Authorization: Bearer <token>`` and the UI
injects it automatically:

.. code-block:: yaml

   settings:
     ui_auth_token: "a-long-random-string"

Keep the default ``127.0.0.1`` bind for anything sensitive, or front it with a
reverse proxy. See :doc:`/http-api` and :doc:`/web-ui`.

Add a new exchange
==================

Implement an adapter under ``dccd/sources/`` with the relevant ``Source``
protocol mixins and a ``capabilities()`` declaration, then register it in
``dccd.application.service_factory.build_registry``. See :doc:`/architecture`.
