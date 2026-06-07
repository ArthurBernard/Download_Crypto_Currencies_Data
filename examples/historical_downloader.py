#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Simple example: download historical data with the v3 async ``Client``.

``Client`` is the one-stop entry point — it wires every exchange adapter and the
local Parquet store, and exposes the four operations as methods: ``backfill``
(download history), ``stream`` (collect live), ``read`` (load stored data) and
``inventory`` (list datasets). Use it as an async context manager so the shared
HTTP client is opened and closed cleanly.

The store root comes from ``settings.data_path`` in your ``config.yml`` (resolved
via the XDG fallback); see ``examples/config.example.yml``.

``start`` accepts:
  - an ISO date string, e.g. ``'2024-01-01'``
  - ``'last'`` (resume from the last stored row)
  - ``'origin'`` (full available history)
"""

import asyncio

from dccd import Client


async def main() -> None:
    async with Client() as c:
        # Download hourly BTC/USDT OHLC for 2024. Re-running only adds what is
        # missing (resume + dedup), so this doubles as an incremental update.
        result = await c.backfill(
            "binance", "BTC/USDT", "ohlc", span=3600, start="2024-01-01"
        )
        print(f"wrote {result['rows_written']} rows")

        # Load what landed on disk (returns a Polars DataFrame).
        df = c.read("binance", "BTC/USDT", "ohlc", span=3600)
        print(df.head())

        # List every stored dataset.
        for dataset in c.inventory():
            print(dataset)


if __name__ == "__main__":
    asyncio.run(main())
