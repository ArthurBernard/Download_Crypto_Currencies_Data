======
BitMEX
======

Capabilities
============

- **Backfill**: OHLC *(bucketed — 1m / 5m / 1h / 1d only)*, trades (full),
  order-book snapshot (``orderBook/L2``).
- **Stream**: OHLC, trades, order book.
- **OHLC fidelity**: ``quote_volume`` — null · ``trades`` — null.

.. note::

   BitMEX only buckets candles at 1m / 5m / 1h / 1d — other spans raise.

Symbols
=======

``XBTUSD`` — BitMEX uses ``XBT`` for Bitcoin.

Example
=======

.. code-block:: python

   async with Client() as c:
       await c.backfill("bitmex", "BTC/USD", "ohlc", span=3600, start="2024-01-01")
       await c.backfill("bitmex", "BTC/USD", "trades", start="2024-01-01")

See :class:`~dccd.sources.bitmex.BitMEXSource` for the full API.
