=====
Bybit
=====

Capabilities
============

- **Backfill**: OHLC (full), order-book snapshot. **No trades.**
- **Stream**: OHLC, trades, order book.
- **OHLC fidelity**: ``quote_volume`` ✅ native · ``trades`` — null.

.. note::

   Bybit spot exposes only the ~60 most recent trades and no history, so trades
   backfill raises :class:`~dccd.domain.errors.NoCapability` rather than
   returning a misleading recent slice. Live trades are available via the stream.

Symbols
=======

``BTCUSDT`` (no separator), spot category.

Example
=======

.. code-block:: python

   async with Client() as c:
       await c.backfill("bybit", "BTC/USDT", "ohlc", span=3600, start="2024-01-01")

API
===

.. autoclass:: dccd.sources.bybit.BybitSource
   :members:
