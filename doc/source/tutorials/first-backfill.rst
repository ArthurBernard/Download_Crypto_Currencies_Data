===================
Your first backfill
===================

In five minutes you will download a few days of Bitcoin hourly candles from
Binance, store them as Parquet, and load them back as a DataFrame. No API key
needed.

.. note::

   New to dccd? This is a *tutorial* — follow it top to bottom. For a specific
   task see the **How-to guides** (in the sidebar); for the exact arguments,
   the :doc:`API reference </api>`.

Install
=======

.. code-block:: bash

   pip install dccd

Step 1 — download some history
==============================

:class:`~dccd.Client` is an async context manager; ``backfill`` downloads
historical data into the local Parquet store and returns how many rows it wrote.

.. code-block:: python

   import asyncio
   from dccd import Client

   async def main():
       async with Client() as c:
           result = await c.backfill(
               "binance", "BTC/USDT", "ohlc", span=3600, start="2026-06-01",
           )
           print(result["rows_written"], "rows")

   asyncio.run(main())

.. code-block:: text

   92 rows

``span=3600`` is the candle size in seconds (1 hour); ``start`` accepts an ISO
date, ``"last"`` (resume from the last stored bar) or ``"origin"`` (full
history).

Step 2 — read it back
=====================

The data is on disk as Parquet. Read it as a `Polars <https://pola.rs>`_
DataFrame (nanosecond ``TS``, sorted, deduplicated):

.. code-block:: python

   async def main():
       async with Client() as c:
           df = c.read("binance", "BTC/USDT", "ohlc", span=3600)
           print(df.select(["TS", "open", "high", "low", "close", "volume"]).head(3))

   asyncio.run(main())

.. code-block:: text

   shape: (3, 6)
   ┌─────────────────────┬──────────┬──────────┬──────────┬──────────┬───────────┐
   │ TS                  ┆ open     ┆ high     ┆ low      ┆ close    ┆ volume    │
   │ ---                 ┆ ---      ┆ ---      ┆ ---      ┆ ---      ┆ ---       │
   │ i64                 ┆ f64      ┆ f64      ┆ f64      ┆ f64      ┆ f64       │
   ╞═════════════════════╪══════════╪══════════╪══════════╪══════════╪═══════════╡
   │ 1780272000000000000 ┆ 73674.39 ┆ 74092.0  ┆ 73654.06 ┆ 73885.0  ┆ 366.4897  │
   │ 1780275600000000000 ┆ 73885.01 ┆ 73916.01 ┆ 73278.02 ┆ 73292.92 ┆ 464.54265 │
   │ 1780279200000000000 ┆ 73292.92 ┆ 73892.52 ┆ 73222.0  ┆ 73790.0  ┆ 506.50025 │
   └─────────────────────┴──────────┴──────────┴──────────┴──────────┴───────────┘

``TS`` is nanoseconds UTC. To get a datetime column:
``df.with_columns(pl.from_epoch("TS", time_unit="ns").alias("time"))``.

What just happened
==================

- The candles were saved under ``{data_path}/binance/ohlc/BTC-USDT/3600s/2026.parquet``
  (one file per year).
- Running the backfill **again** writes nothing new — it is incremental and
  deduplicated. Use ``start="last"`` to top up to now.
- Storage and time semantics are explained in :doc:`/architecture`.

Next steps
==========

- :doc:`stream-live` — record live trades as they happen.
- :doc:`/how-to/schedule-daily` and the other How-to guides — scheduling, deep
  trade history, syncing, migration.
- :doc:`/cli` — do the same from the command line: ``dccd backfill -e binance
  -s BTC/USDT -t ohlc --span 3600 --start 2026-06-01``.
