==========
Quickstart
==========

See :doc:`installation` for prerequisites and install instructions.

----

Python API
==========

Use the async :class:`dccd.Client` as a context manager. It resolves the config
(via ``$XDG_CONFIG_HOME/dccd/config.yml`` or ``./config.yml``) and wires every
exchange adapter.

.. code-block:: python

   import asyncio
   from dccd import Client


   async def main():
       async with Client() as c:
           # Backfill hourly OHLC candles (resumes from the last stored bar).
           result = await c.backfill("binance", "BTC/USDT", "ohlc",
                                     span=3600, start="last")
           print(result["rows_written"], "rows")

           # Read them back as a Polars DataFrame.
           df = c.read("binance", "BTC/USDT", "ohlc", span=3600)
           print(df.tail())


   asyncio.run(main())

``start`` accepts ``"last"`` (resume), ``"origin"`` (full history), an ISO date
(``"2024-01-01"``), or a nanosecond integer. ``data_type`` is ``"ohlc"``,
``"trades"`` or ``"orderbook"``.

Trades backfills are **cursor-paginated**: the engine follows each exchange's
cursor until the requested window is fully drained.

Live streaming
--------------

.. code-block:: python

   import asyncio
   from dccd import Client

   async def main():
       async with Client() as c:
           stop = asyncio.Event()
           asyncio.get_event_loop().call_later(60, stop.set)  # stop after 60s
           await c.stream("binance", "BTC/USDT", "trades", stop_event=stop)

   asyncio.run(main())

CLI
===

.. code-block:: bash

   # One-off backfill
   dccd backfill -e binance -s BTC/USDT -t ohlc --span 3600 --start last

   # Run all stream jobs from the config
   dccd stream

   # Full daemon (scheduler + streams + web UI)
   dccd start

   # Inspect what is stored
   dccd inventory

See :doc:`cli` for the full command reference and :doc:`configuration` for the
YAML config.
