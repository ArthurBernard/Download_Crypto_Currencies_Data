============
Architecture
============

dccd v3 is built as a **hexagonal architecture**: business logic is fully
isolated from I/O and from the interfaces that drive it. Each layer depends only
on the ones beneath it, so the domain can be tested in isolation and a new
exchange, storage backend or interface plugs in without touching the rest.

.. code-block:: text

   Interfaces   CLI  ·  HTTP API  ·  Web UI  ·  Python Client
                                  │
   Application   backfill · stream · read · inventory · Scheduler · Config
                                  │
   Domain  ◄──  Sources (7 adapters)  ◄──  Transport (httpx · WS · Paginator)
                                  │
                              Storage (Parquet · SQLite runs · rclone)

The layers
==========

.. grid:: 1 2 2 3
   :gutter: 3

   .. grid-item-card:: Domain
      :link: api
      :link-type: doc

      Pure, synchronous value objects and transforms — ``Symbol``, ``OHLCBar``,
      ``Trade``, ``Capability`` — with **no I/O**. All timestamps are
      nanoseconds UTC (``int64``).

   .. grid-item-card:: Transport
      :link: api
      :link-type: doc

      Async HTTP (retry/backoff), a reconnecting WebSocket base, a token-bucket
      rate limiter, and the generic cursor/forward **paginator**.

   .. grid-item-card:: Sources
      :link: exchanges
      :link-type: doc

      One adapter per exchange implementing fine-grained ``Source`` protocols,
      declaring their :class:`~dccd.domain.capability.Capability` per
      (data type × transport × mode).

   .. grid-item-card:: Storage
      :link: api
      :link-type: doc

      ``ParquetStore`` (annual/daily files, per-type dedup, provenance,
      atomic writes), an append-only SQLite run history, and rclone sync.

   .. grid-item-card:: Application
      :link: api
      :link-type: doc

      The operations — :func:`~dccd.application.operations.backfill`,
      ``stream``, ``read``, ``inventory`` — plus the async ``Scheduler`` and
      ``EventBus``.

   .. grid-item-card:: Interfaces
      :link: cli
      :link-type: doc

      CLI (Typer), HTTP API (FastAPI) + SSE, the Jinja2 Web UI (a pure HTTP
      client of the API), and the async :class:`~dccd.Client`.

Key design rules
================

- **Domain is pure and synchronous.** It never imports transport, sources or
  storage. This is the only layer under strict ``mypy``.
- **Capabilities are declarative and honoured.** An adapter declares what it can
  do (``history`` depth, ``page_direction``, supported ``spans``, WS channels);
  the engine resolves against them and raises
  :class:`~dccd.domain.errors.NoCapability` early rather than failing midway or
  running an empty stream.
- **Nanosecond UTC timestamps everywhere** (``int64``). Legacy frames are
  normalised on read/merge (``ParquetStore.canonicalize``).
- **The UI is a thin client of the API** — no direct calls into the application
  layer, so the front-end can be replaced without touching business logic.

Data flow — a backfill
======================

1. An interface builds a :class:`~dccd.application.jobs.JobSpec` and calls
   :func:`~dccd.application.operations.backfill`.
2. The operation resolves the exchange adapter from the registry and reads its
   :class:`~dccd.domain.capability.Capability`.
3. The transport **paginator** drives the adapter's ``fetch_*_page`` — by fixed
   windows for OHLC, by cursor for trades (draining the full requested window).
4. Records are flushed in batches to ``ParquetStore``, deduplicated on the
   natural key per data type, with progress emitted on the ``EventBus``.
