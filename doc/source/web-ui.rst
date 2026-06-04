======
Web UI
======

dccd ships a small web UI — a dashboard to watch collection, browse stored data,
run backfills and control streams without touching the CLI. It is a **pure
client of the** :doc:`http-api`: every action is just an API call, so nothing is
hidden from scripting.

.. image:: _static/web-ui-dashboard.png
   :alt: dccd web UI dashboard
   :align: center
   :width: 100%

Running it
==========

.. code-block:: bash

   dccd ui                     # UI + API only (no scheduler)
   dccd start                  # full daemon: scheduler + streams + UI

   dccd ui --host 0.0.0.0 --port 8080

Both read ``settings.ui_host`` / ``ui_port`` from the config. Requires the extra:
``pip install "dccd[daemon,ui]"``. Then open ``http://127.0.0.1:8080``.

To protect it, set ``settings.ui_auth_token`` — the API then requires a bearer
token and the UI injects it automatically. For untrusted networks, keep the
default ``127.0.0.1`` bind and/or put it behind a reverse proxy.

Pages
=====

.. list-table::
   :header-rows: 1
   :widths: 18 82

   * - Page
     - What it shows
   * - **Dashboard**
     - Active runs with a live progress bar, recent run history, stream status, and a datasets summary.
   * - **Inventory**
     - Every dataset on disk under ``data_path``, grouped by exchange, with its row count and time range. Rows covered by a configured job are tagged ``● job``. Launch a backfill from the header or top-up a row's history; order-book rows offer a one-shot **Snapshot**.
   * - **Jobs**
     - Configured jobs grouped by exchange. *Run now* / *Run all* trigger one-shot backfills; *Start* / *Stop* control live streams.
   * - **Config**
     - Edit ``settings`` / ``storage`` / ``alerts`` / ``jobs`` as a form (or raw JSON) and save back to ``config.yml`` — validated server-side.
   * - **Logs**
     - A live console (SSE) that streams a running job's log/progress/status, plus the recent-runs history with each run's log tail.
   * - **Storage**
     - On-disk dataset breakdown by exchange and the v2→v3 migration tool (dry-run first).

The backfill modal
==================

Launching a backfill opens a modal that **polls the run and shows live
progress** (percentage by time covered, the timestamp reached, and the row
count). A long run — e.g. a day of trades is millions of rows — can be cancelled
with **Stop**, which keeps everything already collected. The default ``last``
start uses a bounded look-back (short for trades) so a first click can't trigger
a runaway download.

.. note::

   Inventory lists **all data on disk**, independent of your configured jobs
   (e.g. imported or migrated history). That is by design; the ``● job`` tag
   marks the datasets actually covered by a job.
