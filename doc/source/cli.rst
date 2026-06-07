=============
CLI Reference
=============

The ``dccd`` command line is a thin Typer wrapper over the :doc:`application
layer <api>`. The reference below is generated from the live app, so it always
matches the installed version. The daemon / UI commands need the extra:

.. code-block:: bash

   pip install "dccd[daemon]"

All commands accept ``--config / -c`` to point at a YAML config; without it the
config is resolved from ``./config.yml`` then
``$XDG_CONFIG_HOME/dccd/config.yml``.

Examples
========

.. code-block:: bash

   dccd backfill -e binance -s BTC/USDT -t ohlc --span 3600 --start last
   dccd backfill -e okx -s BTC/USDT -t trades --start 2024-01-01
   dccd stream                 # run configured stream jobs
   dccd start                  # full daemon: scheduler + streams + web UI
   dccd inventory              # list stored datasets

Commands
========

.. click:: dccd.interfaces.cli.main:cli
   :prog: dccd
   :nested: full
