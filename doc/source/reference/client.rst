======
Client
======

The async :class:`dccd.Client` is the one-stop entry point: it wires every
exchange adapter and the local store and exposes the four operations as methods.
Use it as an async context manager.

.. code-block:: python

   import asyncio
   from dccd import Client

   async def main():
       async with Client() as c:
           await c.backfill("binance", "BTC/USDT", "ohlc", span=3600, start="2024-01-01")
           df = c.read("binance", "BTC/USDT", "ohlc", span=3600)
           print(df.tail())

   asyncio.run(main())

.. autoclass:: dccd.Client
   :members:
