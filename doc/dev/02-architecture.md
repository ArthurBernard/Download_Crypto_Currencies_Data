# 2 — Architecture

dccd is a **hexagonal (ports-and-adapters)** design. Dependencies point inward:
the domain knows nothing about I/O; interfaces depend on the application, which
depends on transport/sources/storage, which depend on the domain.

```
interfaces (api · cli · ui)
        │  call
        ▼
application  (operations, Scheduler, EventBus, Config, registry)
        │  use
        ▼
transport · sources · storage
        │  build on
        ▼
domain  (pure: models, capabilities, transforms, timeutils)
```

## Layer by layer

### domain/ — pure, synchronous, no I/O
Never imports from transport/sources/storage. Holds the vocabulary:
- `symbol.py` — `Symbol(base, quote)`, normalises `XBT→BTC`.
- `types.py` — `DataType` enum (`ohlc`, `trades`, `orderbook`).
- `records.py` — `OHLCBar`, `Trade`, `OrderBookSnapshot` (+ `OrderBookLevel`),
  all ns timestamps. `OrderBookSnapshot.bids` are sorted desc, `asks` asc.
- `capability.py` — `Capability`, declared per adapter per
  (data_type × transport × mode): `history` (`full`/`recent`), `max_per_request`,
  `max_depth`, `spans`, `page_direction`.
- `timeutils.py` — `s_to_ns`, `ns_now`, `align_ns`, `binance_interval`, …
  (`NS = 1e9`).
- `transforms.py` — `aggregate_ohlc(trades, span)` (pure computation; basis for
  deriving OHLC from trades).
- `errors.py` — `NoCapability`, `CoverageError`.

### transport/ — async I/O primitives
- `http.py` — `AsyncHTTPClient` (httpx wrapper, retry/backoff). **One
  reference-counted client is shared** across adapters (concurrency-safe).
- `ws.py` — `WebSocketBase`: `stream_raw()` async generator with exponential
  reconnect. Adapters subclass it; they must use `self.stream_raw()`, never roll
  their own reconnect.
- `ratelimit.py` — `RateLimiter` token bucket.
- `paginate.py` — `paginate_ohlc` (forward, window snapped to the bar/span) and
  `paginate_trades` (**cursor-based**: follows each adapter's opaque cursor until
  the window drains).

### sources/ — one adapter per exchange
Each implements the relevant Source protocol mixins from `sources/base.py`:
- REST historical: `OHLCHistory`, `TradesHistory`, `OrderBookSnapshotREST`.
- WS live: `OHLCLive`, `TradesLive`, `OrderBookLive`.

Adapters declare `capabilities()` and implement `fetch_*_page` / `stream_*`. The
**engine rejects anything not declared** (`NoCapability`), so declarations must be
honest. New exchange = add the adapter + register it in
`service_factory.build_registry()`. Per-exchange detail: see `04-exchanges.md`.

### storage/
- `parquet.py` — `ParquetStore`: read/write Parquet with ns timestamps,
  provenance in the footer, and **per-data-type dedup** (`_dedup_subset`:
  OHLC=`TS`, trades=`tid` else composite, order book=`(TS,side,price)`). Reads
  **canonicalize** legacy v2 frames before any `concat` (defensive — never lose
  rows on schema drift). `inventory()` returns rows/time-range plus on-disk
  `bytes`, file count, and (OHLC) `expected_rows`/`missing_rows` gap detection.
- `runs_sqlite.py` — `RunsStore`: SQLite WAL, append-only run history (progress,
  log tail, state) for polling.
- `remote.py` — `RemoteStorage`: rclone sync.

### application/
- `operations.py` — the verbs: `backfill()`, `stream()`, `read()`, `inventory()`.
  `backfill` resolves the adapter + capability and drives the paginator;
  `stream` runs a supervised WS loop and emits throttled liveness samples.
- `config.py` — `AppConfig` + `JobConfig` (Pydantic v2). Runtime CRUD
  (`add_job`/`remove_job`/`update_job_start`) normalises edits to one pair per
  `JobConfig`.
- `events.py` — `EventBus` with **multi-queue fan-out** (many SSE consumers at
  once); events: `ProgressEvent`, `LogEvent`, `StatusEvent`, `StreamSampleEvent`.
- `jobs.py` — `JobSpec`, `JobRun`, `Trigger`, `JobParams`; `JobSpec.make_id`
  builds a stable id (`op:exchange:pair:type[:span]`, **excludes `start`**).
- `scheduler.py` — `Scheduler`: interval/supervised/once orchestration;
  `sync_streams()` reconciles desired vs running stream workers.
- `registry.py` — `REGISTRY`: operation-name → schema (CLI/API/UI parity).
- `monitor.py` — `HealthMonitor`: EventBus subscriber, webhook alerts.
- `service_factory.py` — `build_registry()`/`build_store()`/`build_runs_store()`:
  **the single source of truth for wiring**. `Client`, CLI and API all use it.

### interfaces/
- `api/app.py` — FastAPI `create_app()`. 1:1 with operations; job CRUD at
  `POST /api/jobs/{create,delete,update}` via the async `_persist_and_refresh`
  (writes YAML, updates `app.state`, calls `sync_streams`). SSE at `/api/events`.
- `cli/main.py` — Typer commands; everything imports from `service_factory`.
- `ui/` — Jinja2 templates; a **pure HTTP client of the API** (no direct calls
  into the application layer). See `03-decisions.md` for the page model.

## Request flow (a backfill)

`CLI/API/Client` → `operations.backfill(spec)` → resolve adapter + capability →
choose paginator (OHLC forward / trades cursor) → adapter `fetch_*_page` over
`transport.http` → records flushed in batches to `ParquetStore.save` (dedup +
provenance) → progress/log to `EventBus` + `RunsStore`. Cancellation is
cooperative via a `stop_event` (`DELETE /api/backfill/{id}`).

## Hard invariants (do not regress — mirrored in `CLAUDE.md`)
- Trades pagination is **cursor-based**; OHLC snaps start to the bar, never a
  fixed window for trades.
- Dedup key is **per data type**; `TS` alone is unique only for OHLC.
- Declared capabilities must be **honest**.
- All timestamps **ns UTC int64**; legacy frames pass through `canonicalize()`
  before `concat`.
- First `start=last` backfill is **bounded** per type; backfills are cancellable.
- One **reference-counted** HTTP client shared across adapters.
- `ui_auth_token` enforces Bearer on `/api/*`; CORS is not wildcard.
- Stream workers are **reconciled, not appended** (delete stops + drops them).
- `EventBus` fans out to all queues; SSE consumers add/remove their queue.
