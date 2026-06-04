=============
API Reference
=============

dccd v3 is a hexagonal architecture: a pure, synchronous **domain** with no I/O,
an async **transport** layer, exchange **sources**, **storage**, an
**application** layer of operations, and thin **interfaces** (CLI · HTTP API ·
UI · Python ``Client``). See :doc:`architecture` for the big picture.

Each object below links to its own page with the full signature, parameters and
examples.

Client
======

The one-stop async facade — most users only need this.

.. currentmodule:: dccd

.. autosummary::
   :toctree: generated/
   :nosignatures:

   Client

Domain
======

Pure, synchronous value objects and helpers — no I/O. All timestamps are
nanoseconds UTC (``int64``).

.. currentmodule:: dccd.domain

.. autosummary::
   :toctree: generated/
   :nosignatures:

   symbol.Symbol
   types.DataType
   records.OHLCBar
   records.Trade
   records.OrderBookSnapshot
   records.OrderBookLevel
   capability.Capability
   dataset.DatasetId
   dataset.Provenance

Pure transforms and time helpers:

.. autosummary::
   :toctree: generated/
   :nosignatures:

   transforms.aggregate_ohlc

Sources
=======

One adapter per exchange, implementing the fine-grained ``Source`` protocols,
resolved through a registry. See :doc:`exchanges` for capabilities and fidelity.

.. currentmodule:: dccd.sources

.. autosummary::
   :toctree: generated/
   :nosignatures:

   registry.SourceRegistry
   binance.BinanceSource
   coinbase.CoinbaseSource
   kraken.KrakenSource
   bybit.BybitSource
   okx.OKXSource
   bitfinex.BitfinexSource
   bitmex.BitMEXSource

Transport
=========

Async I/O building blocks shared by every adapter.

.. currentmodule:: dccd.transport

.. autosummary::
   :toctree: generated/
   :nosignatures:

   http.AsyncHTTPClient
   ws.WebSocketBase
   ratelimit.RateLimiter
   paginate.paginate_ohlc
   paginate.paginate_trades

Storage
=======

Parquet datasets (ns timestamps, provenance, per-type dedup) and the run history.

.. currentmodule:: dccd.storage

.. autosummary::
   :toctree: generated/
   :nosignatures:

   parquet.ParquetStore
   runs_sqlite.RunsStore
   migrate.migrate_parquet_to_ns

Application
===========

The operations and orchestration that wire domain, sources and storage together.

.. currentmodule:: dccd.application

.. autosummary::
   :toctree: generated/
   :nosignatures:

   operations.backfill
   operations.stream
   operations.read
   operations.inventory
   scheduler.Scheduler
   config.AppConfig
   config.JobConfig
   jobs.JobSpec
   events.EventBus

Interfaces
==========

.. currentmodule:: dccd.interfaces

.. autosummary::
   :toctree: generated/
   :nosignatures:

   api.app.create_app
