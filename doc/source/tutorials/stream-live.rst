=====================
Streaming live trades
=====================

You will record live BTC/USDT trades from Binance to Parquet for a few seconds,
then stop the stream cleanly. This builds on :doc:`first-backfill`.

Stream for a while, then stop
=============================

:meth:`~dccd.Client.stream` runs until the ``stop_event`` is set. Here we stop
after ten seconds; in a daemon you would set it on shutdown.

.. code-block:: python

   import asyncio
   from dccd import Client

   async def main():
       async with Client() as c:
           stop = asyncio.Event()
           loop = asyncio.get_running_loop()
           loop.call_later(10, stop.set)          # stop after 10 s
           await c.stream("binance", "BTC/USDT", "trades", stop_event=stop)

   asyncio.run(main())

Read what you recorded
======================

.. code-block:: python

   async def main():
       async with Client() as c:
           df = c.read("binance", "BTC/USDT", "trades")
           print(len(df), "trades")
           print(df.select(["TS", "price", "amount", "side"]).head(3))

   asyncio.run(main())

Each row is one trade, deduplicated on its ``tid``, with a nanosecond ``TS``.
Trades land in a per-day file under ``…/binance/trades/BTC-USDT/``.

Run it as a service instead
===========================

Instead of a script, declare the stream as a job in your config and let the
daemon supervise it (auto-reconnect, restart on failure):

.. code-block:: yaml

   jobs:
     - exchange: binance
       pairs: [BTC/USDT]
       data_type: trades
       operation: stream
       trigger_kind: supervised

.. code-block:: bash

   dccd start        # scheduler + streams + web UI

See :doc:`/web-ui` to watch it live and :doc:`/configuration` for the full job
schema.
