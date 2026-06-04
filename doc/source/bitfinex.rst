========
Bitfinex
========

Capabilities
============

- **Backfill**: OHLC, trades, order-book snapshot — up to **10 000 items per
  request** (the largest limit of any adapter).
- **Stream**: OHLC (``candles``), trades. Order-book streaming is not implemented
  and not declared.
- **OHLC fidelity**: ``quote_volume`` — null · ``trades`` — null.

.. note::

   Bitfinex labels Tether ``UST``, so ``BTC/USDT`` is rendered ``tBTCUST``
   (``tBTCUSDT`` returns an empty list). Symbols with a part longer than three
   characters use the ``tBASE:QUOTE`` form.

Symbols
=======

``tBTCUSD`` / ``tBTCUST`` — leading ``t`` for trading pairs; USDT → UST.

Example
=======

.. code-block:: python

   async with Client() as c:
       await c.backfill("bitfinex", "BTC/USDT", "ohlc", span=3600, start="2024-01-01")

See :class:`~dccd.sources.bitfinex.BitfinexSource` for the full API.
