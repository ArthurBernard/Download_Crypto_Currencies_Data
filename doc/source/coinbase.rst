========
Coinbase
========

Capabilities
============

- **Backfill**: OHLC (300 candles/req, windowed automatically), order-book
  snapshot (level 2), trades *(recent only)*.
- **Stream**: trades.
- **OHLC fidelity**: ``quote_volume`` — null · ``trades`` — null.

.. note::

   Coinbase paginates trades through ``CB-AFTER`` response *headers*, which the
   JSON-only transport does not expose — a trades backfill returns a single
   recent page (declared ``history="recent"``). Live OHLC / order book are not
   implemented and not declared.

Symbols
=======

``BTC-USD`` (dash). Coinbase quotes in USD, not USDT.

Example
=======

.. code-block:: python

   async with Client() as c:
       await c.backfill("coinbase", "BTC/USD", "ohlc", span=3600, start="2024-01-01")

API
===

.. autoclass:: dccd.sources.coinbase.CoinbaseSource
   :members:
