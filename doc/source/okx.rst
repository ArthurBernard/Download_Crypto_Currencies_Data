===
OKX
===

Capabilities
============

- **Backfill**: OHLC (full, ``history-candles``), trades (full,
  ``history-trades``, paged backward by timestamp), order-book snapshot.
- **Stream**: OHLC, trades, order book.
- **OHLC fidelity**: ``quote_volume`` ✅ native · ``trades`` — null.

Symbols
=======

``BTC-USDT`` (dash), ``instId`` format.

Example
=======

.. code-block:: python

   async with Client() as c:
       await c.backfill("okx", "BTC/USDT", "trades", start="2024-01-01")

See :class:`~dccd.sources.okx.OKXSource` for the full API.
