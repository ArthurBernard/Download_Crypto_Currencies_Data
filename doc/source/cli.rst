-----------------------------------------
 CLI Reference (:mod:`dccd.daemon.cli`)
-----------------------------------------

The ``dccd`` command-line interface requires the daemon extra:

.. code-block:: bash

   pip install dccd[daemon]

.. tip::

   All commands accept ``--config PATH`` to specify the config file.  When
   omitted, ``dccd`` looks for ``./config.yml`` then falls back to
   ``$XDG_CONFIG_HOME/dccd/config.yml`` (default ``~/.config/dccd/config.yml``).

----

dccd validate
-------------

Check that a config file is valid before using it:

.. code-block:: bash

   dccd validate --config config.yml

Exits with code 0 on success, non-zero on validation errors.  Run this after
every edit to ``config.yml``.

----

dccd backfill
-------------

Download missing historical OHLCV data for all configured pairs:

.. code-block:: bash

   # All exchanges and pairs in the config
   dccd backfill --config config.yml

   # Single exchange
   dccd backfill --config config.yml --exchange binance

   # Single pair
   dccd backfill --config config.yml --exchange binance --pairs BTC/USDT

   # Parallel downloads (one thread per exchange)
   dccd backfill --config config.yml --parallel

Options:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Option
     - Description
   * - ``--exchange NAME``
     - Restrict to one exchange (e.g. ``binance``, ``okx``)
   * - ``--pairs PAIR``
     - Restrict to one pair (e.g. ``BTC/USDT``)
   * - ``--parallel``
     - Run exchanges in parallel threads
   * - ``--start DATE``
     - Override start date (``'2024-01-01'`` or Unix timestamp)
   * - ``--end DATE``
     - Override end date

----

dccd collect
------------

Run WebSocket streams defined under ``stream_jobs:`` in the config:

.. code-block:: bash

   dccd collect --config config.yml

Streams run until interrupted (``Ctrl+C``).  Each stream reconnects automatically
after network errors.

----

dccd start
----------

Start the full daemon: historical scheduler + WebSocket streams + rclone sync:

.. code-block:: bash

   dccd start --config config.yml

This is the main production command.  It combines the APScheduler (histo_jobs)
and StreamManager (stream_jobs) in a single process.  Use a systemd unit or
supervisor to keep it running.

.. code-block:: ini

   [Unit]
   Description=dccd crypto data daemon
   After=network.target

   [Service]
   ExecStart=dccd start --config /etc/dccd/config.yml
   Restart=on-failure
   RestartSec=30

   [Install]
   WantedBy=multi-user.target

----

dccd status
-----------

Print the current health metrics for all running jobs:

.. code-block:: bash

   dccd status --config config.yml

   # Machine-readable JSON (pipe to jq or Grafana)
   dccd status --config config.yml --json

Sample output:

.. code-block:: text

   Job                                       rows   errors  last_run
   ----------------------------------------  -----  ------  --------------------
   binance / BTC-USDT / 3600                 8760       0   2026-05-27 12:00:00
   okx / ETH-USDT / 3600                     8752       1   2026-05-27 12:00:00

----

dccd add
--------

Add a new pair to a histo_job entry and re-validate the config:

.. code-block:: bash

   dccd add --exchange binance --pair BTC-USDT --span 3600

----

dccd remove
-----------

Remove a pair from a histo_job.  If it was the last pair, the whole job entry
is removed:

.. code-block:: bash

   dccd remove --exchange binance --pair BTC-USDT --span 3600

----

dccd inventory
--------------

Scan ``data_path`` and print a table of all stored data with date range, row
count, and gap count per series:

.. code-block:: bash

   dccd inventory --config config.yml

   # Filter by exchange
   dccd inventory --config config.yml --exchange kraken

Sample output:

.. code-block:: text

   Exchange  Pair      Span   Type    Start       End         Rows    Gaps
   --------  --------  -----  ------  ----------  ----------  ------  ----
   binance   BTC-USDT  3600   ohlc    2024-01-01  2026-05-27  20160      0
   kraken    BTC-USD   3600   ohlc    2023-06-01  2026-05-27  25848      3

----

Shell auto-completion
---------------------

Install tab-completion once per shell:

.. code-block:: bash

   dccd --install-completion bash    # or zsh / fish

After restarting the shell (or sourcing the rc file), pressing ``Tab`` completes
subcommand names, ``--exchange`` values, and ``--pairs`` values.

