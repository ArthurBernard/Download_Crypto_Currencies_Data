====================
CryptoHFTData history
====================

`CryptoHFTData <https://cryptohftdata.com/>`_ is an optional historical trade
provider. It reads exchange-native hourly Parquet archives and converts each
trade to dccd's canonical nanosecond UTC schema before the normal Parquet store
persists it.

Install and authenticate
========================

.. code-block:: bash

   pip install dccd[cryptohftdata]
   export CRYPTOHFTDATA_API_KEY=...  # optional

No key is required for the public free tier. An API key removes the public
rate limit and should be supplied through the environment, not committed to a
configuration file.

Provider-qualified venues
=========================

Source names retain both provider and venue provenance:

.. list-table::
   :header-rows: 1

   * - dccd exchange
     - CryptoHFTData venue
     - Pair market
   * - ``cryptohftdata-binance-spot``
     - ``binance_spot``
     - spot
   * - ``cryptohftdata-binance-futures``
     - ``binance_futures``
     - ``:perp``
   * - ``cryptohftdata-bybit-spot`` / ``cryptohftdata-bybit-futures``
     - ``bybit_spot`` / ``bybit``
     - spot / ``:perp``
   * - ``cryptohftdata-kraken-spot`` / ``cryptohftdata-kraken-derivatives``
     - ``kraken_spot`` / ``kraken_derivatives``
     - spot / ``:perp``
   * - ``cryptohftdata-okx-spot`` / ``cryptohftdata-okx-futures``
     - ``okx_spot`` / ``okx_futures``
     - spot / ``:perp``
   * - ``cryptohftdata-bitget-spot`` / ``cryptohftdata-bitget-futures``
     - ``bitget_spot`` / ``bitget_futures``
     - spot / ``:perp``
   * - ``cryptohftdata-hyperliquid-spot`` / ``cryptohftdata-hyperliquid-futures``
     - ``hyperliquid_spot`` / ``hyperliquid_futures``
     - spot / ``:perp``
   * - ``cryptohftdata-lighter`` / ``cryptohftdata-aster-futures`` / ``cryptohftdata-bitmex``
     - ``lighter`` / ``aster_futures`` / ``bitmex``
     - ``:perp``

Usage
=====

.. code-block:: python

   import asyncio
   from dccd import Client

   async def main():
       async with Client() as client:
           result = await client.backfill(
               "cryptohftdata-binance-futures",
               "BTC/USDT:perp",
               data_type="trades",
               start="last",
           )
           print(result)

   asyncio.run(main())

The default first ``start="last"`` trade backfill is bounded to one hour by
dccd. For deeper history, pass an ISO start date. The adapter downloads one
date partition at a time and exposes bounded cursor pages to the application,
so normal progress, flushing, deduplication, resume, and storage behavior apply.

Scope
=====

The adapter exposes historical trades. CryptoHFTData also publishes sequenced
order-book archives, but dccd's REST order-book protocol represents a current
snapshot rather than a historical delta stream; flattening those archives into
snapshots would lose sequence semantics. They are therefore not advertised by
this adapter.
