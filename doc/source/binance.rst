=======
Binance
=======

The reference adapter — full historical depth and every live channel.

Capabilities
============

- **Backfill**: OHLC (``klines``, 1 000/req), trades (``aggTrades``,
  cursor-paginated by ``fromId``), order-book snapshot (``depth``, ≤ 5 000).
- **Stream**: OHLC (``kline``), trades (``aggTrade``), order book (``depth``).
- **OHLC fidelity**: ``quote_volume`` ✅ native · ``trades`` ✅ native.

Symbols
=======

``BTCUSDT`` (no separator) — pass ``BTC/USDT`` to dccd.

Example
=======

.. code-block:: python

   async with Client() as c:
       await c.backfill("binance", "BTC/USDT", "ohlc", span=3600, start="2024-01-01")
       await c.backfill("binance", "BTC/USDT", "trades", start="2024-01-01")

API
===

.. autoclass:: dccd.sources.binance.BinanceSource
   :members:
