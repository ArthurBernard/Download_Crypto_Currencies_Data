=============
CLI Reference
=============

The ``dccd`` command line is a thin Typer wrapper over the application layer.
The daemon/UI commands need the extra:

.. code-block:: bash

   pip install "dccd[daemon]"

All commands accept ``--config / -c`` to point at a YAML config; without it the
config is resolved from ``./config.yml`` then ``$XDG_CONFIG_HOME/dccd/config.yml``.

Commands
========

``dccd backfill``
   One-off historical download.

   .. code-block:: bash

      dccd backfill -e binance -s BTC/USDT -t ohlc --span 3600 --start last
      dccd backfill -e okx -s BTC/USDT -t trades --start 2024-01-01

   Options: ``--exchange/-e``, ``--symbol/-s``, ``--type/-t``
   (``ohlc``/``trades``/``orderbook``), ``--span`` (seconds, OHLC only),
   ``--start`` (``last``/``origin``/ISO date/ns). With no ``-e``/``-s`` it runs
   every ``backfill`` job in the config.

``dccd stream``
   Start every ``stream`` job defined in the config (Ctrl-C to stop).

``dccd start``
   Full daemon: scheduler + supervised streams + web UI. ``--host`` / ``--port``
   override the UI bind address.

``dccd ui``
   Serve only the web UI / HTTP API (no scheduler).

``dccd migrate``
   Migrate legacy v2 Parquet files to the v3 schema. ``--dry-run`` (default)
   previews; ``--no-dry-run`` applies. Idempotent, never drops rows.

``dccd inventory``
   List stored datasets and their file counts.

``dccd status``
   Show the most recent job runs from the runs database.

``dccd validate``
   Validate the config file and report the number of job specs.
