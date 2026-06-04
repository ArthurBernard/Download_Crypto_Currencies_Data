======
Kraken
======

Capabilities
============

- **Backfill**: OHLC *(720 recent bars only)*, trades (full, ``since`` cursor),
  order-book snapshot.
- **Stream**: OHLC, trades, order book (snapshot + deltas reconstructed locally).
- **OHLC fidelity**: ``quote_volume`` ✅ (vwap × volume, exact) · ``trades`` ✅ native.

.. note::

   Kraken's OHLC REST returns only the **720 most recent bars**
   (``history="recent"``); a deeper backfill is **clamped to that window with a
   warning**. Deep OHLC history would have to be derived from trades (deferred).

Symbols
=======

``XXBTZUSD`` — Kraken uses ``XBT`` for Bitcoin and prefixes fiat with ``Z``.
dccd maps ``BTC/USD`` for you.

Example
=======

.. code-block:: python

   async with Client() as c:
       await c.backfill("kraken", "BTC/USD", "trades", start="2024-01-01")  # full history
       await c.backfill("kraken", "BTC/USD", "ohlc", span=3600, start="last")  # 720 recent

See :class:`~dccd.sources.kraken.KrakenSource` for the full API.
