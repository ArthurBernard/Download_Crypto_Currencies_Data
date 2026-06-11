========
HTTP API
========

``dccd ui`` (UI only) and ``dccd start`` (full daemon) serve a JSON HTTP API
built with FastAPI. The :doc:`web-ui` is a pure client of this API, so anything
the UI does, you can script.

Base URL defaults to ``http://127.0.0.1:8080`` (``settings.ui_host`` /
``ui_port``). Interactive OpenAPI docs are available at ``/docs``.

Authentication
==============

If ``settings.ui_auth_token`` is set, every ``/api/*`` route requires a bearer
token (``/health`` and the UI pages stay open):

.. code-block:: bash

   curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8080/api/inventory

Server-Sent Events cannot send headers, so ``/api/events`` also accepts the
token as a query parameter: ``/api/events?token=$TOKEN``. CORS is same-origin by
default (configurable via ``settings.ui_allow_origins``).

Endpoints
=========

Inventory & data
----------------

.. list-table::
   :header-rows: 1
   :widths: 18 30 52

   * - Method
     - Path
     - Description
   * - ``GET``
     - ``/api/inventory``
     - All stored datasets (exchange, pair, type, span, rows, time range, on-disk ``bytes``, file count; OHLC also gets ``expected_rows`` / ``missing_rows`` for candle-coverage computation — ``missing_rows = expected_rows − actual_rows`` counts clock-time slots with no candle, which is normal for illiquid pairs that emit no empty candles).
   * - ``POST``
     - ``/api/read``
     - Read stored rows for a dataset; body ``{exchange, symbol, data_type, span, start_ns, end_ns}`` (returns up to 1 000 rows).

Backfill
--------

.. list-table::
   :header-rows: 1
   :widths: 18 30 52

   * - Method
     - Path
     - Description
   * - ``POST``
     - ``/api/backfill``
     - Launch a backfill; body ``{exchange, symbol, data_type, span?, start}``. Returns ``{run_id}``.
   * - ``GET``
     - ``/api/backfill/{run_id}``
     - Poll a run: ``state``, ``rows_written``, ``progress`` (time-based), ``log_tail``.
   * - ``DELETE``
     - ``/api/backfill/{run_id}``
     - Cancel a running backfill (cooperative — stops at the next page, keeps collected rows).

.. code-block:: bash

   # start, then poll
   RID=$(curl -s -XPOST localhost:8080/api/backfill \
     -H 'Content-Type: application/json' \
     -d '{"exchange":"binance","symbol":"BTC/USDT","data_type":"ohlc","span":3600,"start":"2024-01-01"}' \
     | jq -r .run_id)
   curl -s localhost:8080/api/backfill/$RID | jq '{state, rows_written, progress}'

Streams
-------

.. list-table::
   :header-rows: 1
   :widths: 18 32 50

   * - Method
     - Path
     - Description
   * - ``GET``
     - ``/api/streams``
     - Configured stream jobs and their running state.
   * - ``POST``
     - ``/api/streams/start``
     - Start a stream; body ``{spec_id}``.
   * - ``POST``
     - ``/api/streams/stop``
     - Stop a stream; body ``{spec_id}``.

Jobs
----

.. list-table::
   :header-rows: 1
   :widths: 18 28 54

   * - Method
     - Path
     - Description
   * - ``GET``
     - ``/api/jobs``
     - All configured job specs + state (incl. ``start``, ``every``, ``snapshot_interval``, ``depth``).
   * - ``POST``
     - ``/api/jobs/create``
     - Add a job; body ``{operation, exchange, symbol, data_type, span?, start?, trigger_kind?, every?, snapshot_interval?, depth?}``. Returns ``{job_id}``.
   * - ``POST``
     - ``/api/jobs/update``
     - Change a job's start (first date); body ``{job_id, start}``.
   * - ``POST``
     - ``/api/jobs/delete``
     - Remove a job (stored data is kept); body ``{job_id}``.
   * - ``POST``
     - ``/api/jobs/run``
     - Trigger one configured backfill job; body ``{job_id}``.
   * - ``POST``
     - ``/api/jobs/run-all``
     - Trigger every enabled backfill job.

All three mutating routes persist to ``config.yml`` and reconcile stream workers
(a deleted stream's worker is stopped and dropped).

Config, events & storage
------------------------

.. list-table::
   :header-rows: 1
   :widths: 18 26 56

   * - Method
     - Path
     - Description
   * - ``GET`` / ``PUT``
     - ``/api/config``
     - Read / replace the configuration (validated; persisted to ``config.yml``).
   * - ``GET``
     - ``/api/events``
     - **SSE** stream of live ``log`` / ``progress`` / ``status`` / ``sample`` events (``sample`` carries a stream's last ``value`` or ``bid``/``ask`` for liveness). Fans out to multiple concurrent consumers.
   * - ``GET``
     - ``/api/operations``
     - List registered operations.
   * - ``GET``
     - ``/health``
     - Liveness probe (always open).

The application factory is :func:`~dccd.interfaces.api.app.create_app`.
