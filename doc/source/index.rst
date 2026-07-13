.. raw:: html

   <div class="hero-header">
     <img class="only-light" src="_static/logo-light-transparent.svg" alt="dccd logo">
     <img class="only-dark"  src="_static/logo-dark-transparent.svg"  alt="dccd logo">
     <h1 class="hero-title">Download Crypto Currencies Data</h1>
   </div>

.. rst-class:: hidden-rst-title

====================================================
 Download Crypto Currencies Data
====================================================

.. raw:: html

   <div class="badge-row">
     <img src="https://img.shields.io/pypi/pyversions/dccd.svg" alt="Python versions">
     <a href="https://pypi.org/project/dccd/"><img src="https://img.shields.io/pypi/v/dccd.svg" alt="PyPI version"></a>
     <a href="https://pypi.org/project/dccd/"><img src="https://img.shields.io/pypi/status/dccd.svg?colorB=blue" alt="PyPI status"></a>
     <a href="https://github.com/ArthurBernard/Download_Crypto_Currencies_Data/actions/workflows/ci.yml"><img src="https://github.com/ArthurBernard/Download_Crypto_Currencies_Data/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
     <a href="https://github.com/ArthurBernard/Download_Crypto_Currencies_Data/blob/master/LICENSE.txt"><img src="https://img.shields.io/github/license/ArthurBernard/Download_Crypto_Currencies_Data.svg" alt="License"></a>
     <a href="https://download-crypto-currencies-data.readthedocs.io/en/latest/"><img src="https://readthedocs.org/projects/download-crypto-currencies-data/badge/?version=latest" alt="Documentation"></a>
     <a href="https://codecov.io/gh/ArthurBernard/Download_Crypto_Currencies_Data"><img src="https://codecov.io/gh/ArthurBernard/Download_Crypto_Currencies_Data/branch/master/graph/badge.svg" alt="Coverage"></a>
     <a href="https://github.com/ArthurBernard/Download_Crypto_Currencies_Data"><img src="https://raw.githubusercontent.com/ArthurBernard/Download_Crypto_Currencies_Data/badges/interrogate_badge.svg" alt="Docstring coverage"></a>
     <a href="https://pepy.tech/project/dccd"><img src="https://pepy.tech/badge/dccd" alt="Downloads"></a>
   </div>

``dccd`` downloads crypto-currency data (OHLCV, trades, order book) from
8 direct exchange adapters, with optional CryptoHFTData historical venues, and
stores it as nanosecond-precision Parquet — backfill history or
stream live, from Python, a CLI, or a web UI. No API key required.

.. code-block:: bash

   pip install dccd

.. code-block:: python

   import asyncio
   from dccd import Client

   async def main():
       async with Client() as c:
           await c.backfill("binance", "BTC/USDT", "ohlc", span=3600, start="2024-01-01")
           print(c.read("binance", "BTC/USDT", "ohlc", span=3600).tail())

   asyncio.run(main())              # → a Polars DataFrame of hourly candles

New here? Start with :doc:`tutorials/first-backfill`.

.. grid:: 1 1 2 3
   :gutter: 3

   .. grid-item-card:: Python API
      :link: quickstart
      :link-type: doc

      Historical REST downloads and real-time WebSocket streams — use
      ``dccd`` directly in your scripts or notebooks.

   .. grid-item-card:: CLI & Daemon
      :link: cli
      :link-type: doc

      ``dccd`` command line: ``backfill``, ``stream``, ``start`` (daemon +
      web UI), ``inventory`` — driven by a YAML config.

   .. grid-item-card:: API Reference
      :link: api
      :link-type: doc

      The hexagonal layers — domain, transport, sources, storage,
      application, interfaces.

.. rubric:: Key features

- **8 direct adapters** — Binance, Coinbase, Kraken, Kraken Futures, Bybit, OKX,
  Bitfinex, BitMEX — plus 15 optional CryptoHFTData historical venue adapters
- **3 data types** — OHLCV candles, trade history, order book snapshots
- **Async-first** — ``async with Client() as c: await c.backfill(...)``; httpx + websockets
- **Cursor-paginated trades** — backfills drain the full window (no silent loss)
- **Incremental & idempotent** — ``start="last"`` resumes from the last bar; dedup on the natural key
- **Nanosecond Parquet storage** — ns UTC ``int64``, provenance, atomic writes; read back as a :class:`polars.DataFrame`
- **No API key required** — all endpoints used are public
- **Autonomous daemon** — YAML config, async scheduler, WebSocket streams, rclone remote sync

.. rubric:: Guides

.. grid:: 1 2 2 2
   :gutter: 3

   .. grid-item-card:: Architecture
      :link: architecture
      :link-type: doc

      The hexagonal layers and how a backfill flows through them.

   .. grid-item-card:: Exchanges
      :link: exchanges
      :link-type: doc

      Per-exchange capabilities and OHLC field fidelity.

.. _supported-exchanges:

Supported exchanges
-------------------

You pick a **data type** (OHLC · trades · order book) and an **operation** —
**backfill** (download history) or **stream** (collect live). Each cell lists
the data types an exchange supports for that operation.

.. list-table::
   :header-rows: 1
   :stub-columns: 1
   :widths: 16 44 40

   * - Exchange
     - Backfill (history)
     - Stream (live)
   * - Binance
     - OHLC · trades · book
     - OHLC · trades · book
   * - Coinbase
     - OHLC · book · trades [#recent]_
     - trades
   * - Kraken
     - OHLC [#kr]_ · trades · book
     - OHLC · trades · book
   * - Bybit
     - OHLC · book
     - OHLC · trades · book
   * - OKX
     - OHLC · trades · book
     - OHLC · trades · book
   * - Bitfinex
     - OHLC · trades · book
     - OHLC · trades
   * - BitMEX
     - OHLC · trades · book
     - OHLC · trades · book
   * - CryptoHFTData venues
     - trades
     - —

.. [#recent] Coinbase trades backfill returns recent trades only (no deep
   history). Bybit spot has no trade history at all.
.. [#kr] Kraken OHLC backfill serves the 720 most recent bars; a deeper request
   is clamped to that window. Order-book "backfill" is a single point-in-time
   snapshot.

See :doc:`exchanges` for per-exchange notes and OHLC field fidelity.
Bybit spot has no trade history at all (WS only). All other trade backfills are
cursor-paginated and drain the full requested window.

.. toctree::
   :hidden:
   :caption: Getting Started

   installation
   quickstart

.. toctree::
   :hidden:
   :caption: Tutorials

   tutorials/first-backfill
   tutorials/stream-live

.. toctree::
   :hidden:
   :caption: How-to guides

   how-to/schedule-daily
   how-to/deep-trades
   how-to/derivatives
   how-to/analyse
   how-to/sync-remote
   how-to/protect-ui
   how-to/deploy
   how-to/expose-remote
   how-to/add-exchange

.. toctree::
   :hidden:
   :caption: Concepts

   architecture
   exchanges

.. toctree::
   :hidden:
   :caption: Reference

   api
   cli
   http-api
   configuration
   web-ui

.. toctree::
   :hidden:
   :caption: About

   changelog
