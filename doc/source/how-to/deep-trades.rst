==================================================
Backfill deep trade history (and cancel a runaway)
==================================================

Trades are cursor-paginated and drain the **whole** window — a day of a liquid
pair is millions of rows. Give an explicit ``start`` date:

.. code-block:: bash

   dccd backfill -e okx -s BTC/USDT -t trades --start 2024-01-01

From the web UI, the **Historical** page shows the run's live progress on the
dataset's coverage bar, and **Run** turns into **Stop** to cancel it (everything
already collected is kept). Via the API:

.. code-block:: bash

   curl -XDELETE localhost:8080/api/backfill/$RUN_ID
