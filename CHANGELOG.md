# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Changed

### Fixed

- Kraken adapter maps pairs by **altname** (`{base}{quote}`, with `BTC→XBT` and
  `DOGE→XDG`) instead of legacy X/Z-prefixed codes, so OHLC and trades for modern
  Kraken assets (TRX, DOT, BNB, …) and Dogecoin no longer fail with `Unknown asset
  pair`. Legacy pairs are unaffected — Kraken accepts altnames universally and the
  response is parsed by its code-key fallback. (#169)

### Deprecated

### Removed

## [3.6.1] - 2026-06-19

### Fixed

- `ParquetStore.save()` now rejects rows whose timestamp is null or `<= 0` at the
  storage write boundary (one central guard for every adapter and data type), so a
  lost/epoch-0 timestamp can no longer be persisted or poison gap detection — a
  single `TS=0` bar otherwise dragged `inventory()` `min_ts` to `1970` and inflated
  `expected_rows`/`missing_rows` to a bogus ~89 %. Dropped rows are logged, not
  raised, so one bad bar can't abort a good page. (#165)

## [3.6.0] - 2026-06-13

### Added

### Changed

- The web UI **Dashboard** is now a health-first operations view: a status-chip
  line (fresh `<24h` / coverage gaps / failures-24h / last-collection), a
  **Needs attention** panel that surfaces failed runs (with their error reason)
  and datasets with coverage gaps — each with a one-click **Retry/Fill gaps**
  when a configured job matches, else a deep-link to Historical — plus a
  compact Active-now, recent runs, and a per-exchange Data summary with
  freshness dots. Client-side only (inventory + runs + jobs); no API change.
  The Needs-attention panel intentionally treats OHLC coverage gaps as
  actionable (revising the scope of #132 for the triage surface). (#157)
- Web UI visual refresh ("instrument panel" direction), all in `base.html`
  tokens so every page inherits it: **self-hosted** Martian Mono (wordmark +
  section labels) and Spline Sans (body) woff2 served from `/static/fonts`
  (latin subset) — no external font CDN, works fully offline and leaks nothing;
  tabular figures in tables/chips/totals, machined-panel card depth, a faint
  top glow, and one staggered page-load reveal (`prefers-reduced-motion`
  respected). (#158)

### Fixed

- Self-hosted UI fonts are now actually packaged in the wheel: `package-data`
  listed only `static/*` (top level), which excluded the `static/fonts/`
  subdirectory added for the visual refresh — a pip-installed UI would 404 on
  its `.woff2` and silently fall back to system fonts. Verified by inspecting
  the built wheel (6 woff2 present). (#160)
- The **Live** page no longer hangs on "Loading…" when no stream jobs are
  configured (a fresh install, or a backfill-only setup): the structure-change
  guard initialised `lastSig` to `''`, which equals the signature of an empty
  job set, so the very first `load()` matched and returned before the panes
  ever rendered their "No … streams yet" empty state. `lastSig` now starts at
  `null` so the first render always runs. (#159)

### Deprecated

### Removed

## [3.5.2] - 2026-06-12

### Added

- Boot-time runs.db retention (`settings.runs_retention_days`, default 90,
  `0` disables): terminal non-failed runs (`succeeded`/`stale`/`cancelled`)
  older than the window are deleted and the database VACUUMed at daemon
  start, right after the orphan sweep; `failed` runs are kept as the
  long-term error journal. Verified on a copy of the production runs.db:
  1,770 old rows pruned, file size −67 %, `failed` rows untouched. (#154)

### Fixed

- Webhook alerts send a plain-text body with `X-Title: dccd` /
  `X-Priority: high` headers for ntfy-style endpoints — the phone showed a
  raw JSON blob before; Slack webhooks (`hooks.slack.com`) keep the JSON
  `{"text": …}` payload. Verified live: one test message delivered to the
  production ntfy topic (HTTP 200) rendered as plain text. (#155)
- Manual backfill triggers (`POST /api/backfill`, `/api/jobs/run`,
  `/api/jobs/run-all`) are idempotent: a spec that is already being
  backfilled returns the existing `run_id` (`status: already-running`) —
  run-all skips busy jobs and lists them under `already_running` — instead
  of starting a duplicate concurrent run that wasted exchange requests and
  confused runs/progress. (#153)
- Off-box sync no longer mirrors deletions: `RemoteStorage` runs
  `rclone copy` instead of `rclone sync`, so locally purged files survive
  on the remote for read-through restore — enabling `min_free_gb` no longer
  risks deleting the only copy of old data. The remote is now an archive
  superset (never deleted automatically; remote cleanup is manual).
  Verified live against a real rclone remote: purge → sync → file survives
  → `restore()` returns byte-identical content. (#152)

## [3.5.1] - 2026-06-12

### Fixed

- `dccd start` marked its own just-started stream runs `stale` at boot: the
  orphan sweep (`mark_stale_running`) ran in the FastAPI lifespan *after*
  `cmd_start` had already started the scheduler's stream workers, so their
  fresh `running` rows were swept as "orphaned by daemon restart" and the
  Dashboard "Active now" never showed streams. The sweep now runs in
  `cmd_start` before the scheduler starts; the lifespan only sweeps in
  standalone `dccd ui`. Verified live across two daemon launches: the live
  run stays `running`; a restart stales only the previous one. (#145)
- OKX OHLC pagination silently dropped the bar at every 100-bar page
  boundary: OKX `before`/`after` cursors are exclusive, so passing
  `before=start_ms` excluded the bar exactly at each window start (observed
  in production as 431 one-minute gaps per OKX pair, spaced exactly
  100 min). `fetch_ohlc_page` now sends `before=start_ms-1`; regression
  test drives the paginator across a page boundary under faithful exclusive
  semantics. Verified live: a 12 h OKX 1m backfill lands with 0 gaps and
  all 7 boundary bars present. (#144)

## [3.5.0] - 2026-06-11

### Added

- Proactive per-exchange rate limiting on all REST fetches: the (previously
  unwired) token-bucket `RateLimiter` is now a process-wide singleton keyed
  by exchange, awaited before every outbound request — concurrent operations
  on the same exchange share one bucket, so a `run-all` burst stays under
  the exchange's published rate (reactive 429/Retry-After handling remains
  as a backstop). Defaults verified against official docs (Coinbase's old
  10/s constant was wrong — public cap is 3/s). Verified live on Kraken:
  31 pages in 30 s = 1.03 req/s vs a 1.0/s cap, zero 429; 3 concurrent
  backfills total 1.10 req/s, not 3×. (#130)
- CLI test suite (`test_cli.py`, Typer `CliRunner`, 27 tests): every command
  covered offline — `backfill` runs end-to-end through a fake adapter and
  the real `ParquetStore`, exit codes and failure modes asserted.
  `interfaces/cli/main.py` goes from 0 % to 97 % coverage with zero
  production-code changes. (#134)
- Offline adapter-parsing tests from 14 recorded live payloads (REST + WS,
  capture commands + dates in `tests/v3/fixtures/README.md`), with pure
  `_parse_*` seams extracted (behavior-preserving) in the binance, coinbase,
  bitfinex and bitmex adapters; plus WS reconnect tests for
  `WebSocketBase.stream_raw()`. Coverage: bitfinex 38→71 %, bitmex 39→67 %,
  coinbase 42→71 %, binance 50→77 %, `transport/ws.py` 44→93 %; parser ↔
  live agreement cross-checked on capture day. (#135)

### Changed

- Deploy template (`deploy/dccd.service`) ships an explicit
  `TimeoutStopSec=120` (62 s stop observed in production vs the 90 s systemd
  default before SIGKILL) and a commented `MemoryMax=1.5G` example (prod RSS
  ~830 MB with 50 jobs); the deploy and sync-remote how-tos gain a
  "Production checklist" (off-box backup, alert webhook, systemd limits).
  Limits applied and verified live on the production collector. (#131)
- Data page presents OHLC completeness as neutral "candle coverage" (with an
  explanatory tooltip) instead of a red "missing %": exchanges emit no candle
  for minutes without trades, so an illiquid pair showed an alarming
  "85 % missing" when nothing was lost. True holes still surface (the number
  drops); `expected_rows`/`missing_rows` API fields are unchanged. (#132)

### Fixed

- The OpenAPI schema (`/docs`, `/openapi.json`) reports the real installed
  package version instead of a hardcoded "3.0.0"; CI gains a `docs` job that
  builds Sphinx with `-W` so the "0 warnings" rule is enforced on every PR
  (proven to fail on an injected broken reference). (#133)
- Stream jobs without a live WS capability no longer leak zombie `running`
  rows in runs.db (the capability check now runs before `create_run`), and
  their supervisor stops instead of retrying a permanent error every 60 s;
  the stream restart backoff resets to 5 s after a healthy run (≥ 300 s).
  (#126)
- Runs orphaned in `running` state by a daemon crash/SIGKILL are marked
  `stale` (with `error='orphaned by daemon restart'`) at daemon boot instead
  of polluting `active_runs()`, `dccd status` and the Dashboard forever
  (production had ~350 such rows). Boot-only: one-shot CLI commands never
  trigger the purge. (#127)
- Trades/OHLC streams flush to disk on a 60 s interval as well as on the
  1000-record threshold — a quiet pair no longer keeps hours of data in RAM
  (lost on crash, invisible to inventory/freshness). Stream runs now record
  their real `rows_written` (was always 0) on every finish path. Verified
  live: mid-stream parquet at ~63 s, `rows_written` = on-disk rows exactly.
  (#128)
- Paginated backfills no longer open a fresh `httpx.AsyncClient` (TCP pool +
  TLS handshake) per page: `backfill()` holds the adapter's ref-counted HTTP
  client open for the whole operation, and the `Client` context manager now
  actually opens the shared pools on enter and closes them on exit (its
  `__aexit__` was a `pass`). Verified live: a ~42-page Coinbase backfill
  constructs 1 pool instead of ~42. (#129)

### Deprecated

### Removed

## [3.4.0] - 2026-06-10

### Added

### Changed

- Remote-friendly UI transport: gzip on API/page responses (`/api/inventory`
  measured 12 450 B → 460 B, 27×; SSE excluded and flushed immediately so
  EventSource connects without waiting for the first event), dashboard fetches
  parallelised (3×RTT → 1×RTT), Live re-fetches the inventory only on load and
  stream-status changes instead of every 8 s, dashboard/storage poll cadences
  relaxed (15 s / 30 s), and runs-store SQLite reads moved off the event loop.
  UI smoke: 27/27 steps clean. (#121)

### Fixed

- Order-book stream jobs with a depth the exchange doesn't support (production
  case: Kraken `depth: 20`/`50` — WS v2 only accepts {10, 25, 100, 500, 1000})
  were silently rejected and sat "live" forever writing nothing. Valid depths
  are now declared per capability (verified against the live APIs), requests
  snap to the nearest valid value with a warning, and WS subscription
  rejections raise — surfacing in runs as `failed` with the exchange's error —
  instead of being filtered out. (#122)
- A live stream whose WS generator ended on its own (no stop requested) was
  recorded `cancelled`; it is now `failed` with an explicit
  "stream ended unexpectedly" error, so Logs/Runs no longer claim someone
  stopped a stream nobody touched. (#121)
- Order-book WS adapters built the full book as pydantic objects on **every**
  delta while the stream operation kept only one frame per `snapshot_interval` —
  97.7 % CPU on the production collector, starving the event loop and making
  the remote UI unusable. Snapshots are now constructed only at capture time
  (`min_interval` pushed down into the adapters; `0.0` keeps per-frame), and
  Kraken/Bybit book state is truncated to the subscribed depth (WS truncation
  contract). Verified live: Kraken 60 s at 10 s interval → exactly 6 snapshots,
  ≤ 25 levels/side, never crossed; 3 live books for 2 min → **2.0 %** CPU. (#120)
- `ParquetStore` metadata (inventory, last timestamp, gap detection) no longer
  reads the full TS column of every file in the store — it reads parquet footer
  statistics with a per-file mtime cache (legacy files without statistics fall
  back to the old path), and `/api/inventory` + `/api/storage/sync` now run it
  off the event loop. Verified value-identical on real data; 13× faster warm on
  a small store, and the gap grows with file size (production showed 100 s for
  a 10 KB inventory response under load). (#119)
- Permanently failing scheduled jobs no longer hammer the exchange at full
  cadence: the interval loop applies exponential backoff (reset on success) and
  starts with a random jitter instead of firing every job at once on daemon
  start. `HealthMonitor` alerts once when the failure threshold is crossed,
  then at most hourly — instead of on every failure (a broken job used to spam
  the webhook every ~20 s). (#118)

### Deprecated

### Removed

## [3.3.1] - 2026-06-10

### Fixed

- Kraken crypto/crypto OHLC pairs (e.g. `ETH/BTC`): `_kraken_pair` only mapped the
  *base* `BTC→XBT`, producing `XETHXBTC` which Kraken rejects with "Unknown asset
  pair"; it now maps the *quote* too (`XETHXXBT`). Verified against the live Kraken
  API (721 bars). (#113)

## [3.3.0] - 2026-06-10

### Added

- Threat-model section in `how-to/expose-remote` (trust boundaries: localhost / tailnet
  / public; what the token+cookie session protect and don't; residual risks; a
  recommended-postures table) — completing **Epic B** (view the UI remotely). (#110)
- API hardening for remote exposure (all opt-in, off by default): `ui_rate_limit`
  (token-bucket per client on `/api/*`, over budget → `429` + `Retry-After`),
  `ui_readonly` (block mutating methods → `403`, view-only share), and
  `ui_trusted_proxy` (trust `X-Forwarded-For` as the rate-limit key only behind a
  vetted proxy). Regression tests prove CORS is never wildcard and every mutating
  route is `401` without a token. Verified live over Tailscale. (#108)
- Browser login + session: when `ui_auth_token` is set, the web UI now serves a
  `/login` page and an `HttpOnly`, `SameSite=Lax` session cookie (marked `Secure`
  behind an HTTPS proxy), with a Logout control. Page routes are gated (an
  unauthenticated load is redirected to `/login`, no longer served), and the API
  accepts the cookie alongside `Bearer`/`?token=`. Verified live over Tailscale. (#107)
- How-to guide `how-to/expose-remote` for reaching the UI from a laptop/phone behind
  TLS (Caddy/nginx/Cloudflare Tunnel) or a private Tailscale overlay — never the API
  plaintext off-box. Verified on a real server (Tailscale path reached live; Caddy
  installs + reverse-proxies). (#106)

### Changed

- Responsive layout for the web UI on narrow (mobile) viewports: wide/dense tables
  scroll inside their own box (a `MutationObserver` wraps tables built after fetch),
  bigger tap targets, and tighter nav/chrome under 640px — desktop layout unchanged.
  `ui_smoke.py` gains a 390px mobile pass asserting no page-wide horizontal overflow.
  Verified: 27/27 smoke steps, Δ=0px overflow on every page. (#109)

### Fixed

- The web UI no longer injects the raw `ui_auth_token` into served pages (it was
  templated into `base.html`); a remotely reachable UI could leak the token to anyone
  who loaded a page. The browser now holds only an opaque session cookie. (#107)

### Deprecated

### Removed

## [3.2.0] - 2026-06-10

### Added

- Dev workflow: hierarchical, file-based **plan trees** under `doc/dev/plans/`
  (committed) with a `<plans_dir>` descriptor key. A roadmap item expands into a
  global `00-plan.md` + precise leaf specs (adaptive depth); each leaf declares a
  `complexity` that derives its execution model (`low→haiku`/`medium→sonnet`/
  `high→opus`). New `/plan` (build the tree + open the plan PR first) and
  `/execute-leaf` (spawn an agent per leaf, verify on real data) skills; `/pick-task`,
  `/finish-task`, `/abandon-task`, `/release` and `CLAUDE.md` updated to chain
  through it. Backward-compatible: no `plans_dir` ⇒ the old plan-mode loop. (#94)
- Restart/reboot safety verified on a real server `systemctl reboot`: the daemon
  auto-starts, the trades stream reconnects, the interval backfill re-arms, the
  `RunsStore` (SQLite WAL) survives and appends, and the coverage manifest keeps the
  resume cursor (no gap). New `test_restart.py` guards RunsStore persistence across a
  reopen and scheduler interval re-arm from config. (#99)
- Ops for unattended deploy: `HealthMonitor` is now wired into the daemon (CLI
  `dccd start` and the standalone API) — it was implemented but never instantiated,
  so webhook alerts never fired. Docker `HEALTHCHECK` on `/health`, commented
  systemd resource limits, and journald log-rotation guidance. Verified live on a
  server: a failing job past the threshold delivered a real webhook POST, and the
  container reports `healthy`. (#100)
- Docs: new `how-to/deploy` guide — a blessed, host-validated path to run dccd
  unattended on a server (systemd + venv recommended, Docker alternative), covering
  install, secret injection, `/health`, restart/reboot safety, logs, alerts and the
  old-CPU caveat. Completes **Epic A** (run on a remote server). (#102)

### Changed

- `Dockerfile`: pin the base image to a digest (reproducible builds) and add a
  `POLARS_VARIANT` build arg — on CPUs without AVX2 (older servers) the default
  `polars` wheel crashes with SIGILL, so
  `docker build --build-arg POLARS_VARIANT=polars-lts-cpu` installs the LTS-CPU
  build instead. Verified end-to-end on a real host (build, run, `/health`, Bearer
  auth, a backfill writing correct OHLC to the `/data` volume). (#97)
- Docs: `how-to/protect-ui` now covers deploy-time secret injection — the token and
  `rclone.conf` are mounted at run time, never baked into the image (verified on the
  built image: `docker history`/filesystem show no config); the YAML loader does not
  expand `${ENV}` placeholders, so the mounted-file pattern is the blessed one. (#101)

### Fixed

- `deploy/dccd.service`: `ExecStart` pointed at `/usr/local/bin/dccd` and failed
  `systemd-analyze verify`; it now uses a venv path (`/opt/dccd/venv/bin/dccd`) with
  `StateDirectory=dccd` (systemd owns `/var/lib/dccd`). The install spec dropped the
  non-existent `ui` extra (`.[daemon,ui]` → `.[daemon]`, also in the `Dockerfile`).
  Verified a real system-wide install: `systemd-analyze verify` passes, the service
  is active, auto-restarts after SIGKILL, and a backfill writes correct OHLC under
  the hardened `/var/lib/dccd/data` (`ProtectSystem=strict`). (#98)
- `HealthMonitor` counted consecutive failures per `run_id`, but each backfill run
  has a unique id (`{spec}@{ts}`), so repeated failures never accumulated (only
  streams, with a stable `@stream` id, could alert). It now keys on the job
  (spec id) so repeated backfill failures trip the alert. (#100)

### Deprecated

### Removed

## [3.1.0] - 2026-06-09

### Added

- `dccd start` now schedules rclone remote sync: when `storage.remotes` is set,
  the daemon mirrors the store off-box every `storage.sync_interval` seconds with
  exponential backoff, persisted run history (`sync` runs in `RunsStore`) and a
  live `remote-sync` EventBus status. Previously `RemoteStorage` was implemented
  but never driven — a server synced nothing. (#86)
- Storage page surfaces remote sync: last/next sync, status, configured remotes
  and synced volume, plus a **Sync now** button — backed by
  `GET`/`POST /api/storage/sync`. The shared `operations.sync_remote` primitive
  records each cycle, so the manual button and the scheduled loop stay in sync. (#87)
- Coverage manifest (`CoverageStore`, SQLite under `.dccd/`): backfill records each
  dataset's `[min_ts, max_ts]` extent, and `start="last"` falls back to the
  manifest's `max_ts` when no local file exists — so local data can be dropped to
  free disk without forcing a re-download on the next backfill. (#88)
- Free-space purge: `storage.min_free_gb` (default `0` = off). After each
  successful sync the daemon drops the oldest already-synced Parquet files until
  free space is back above the floor (the coverage manifest keeps the resume
  cursor, `.dccd/` is never touched). (#89)
- Read-through restore: reading a dataset whose local Parquet was purged now pulls
  it back from the remote (`rclone copy`) before loading, so a purge is
  transparent to readers (`Client.read`, `POST /api/read`). (#90)
- Docs: the `how-to/sync-remote` guide now covers rclone provisioning, the
  `min_free_gb` free-space purge, read-through restore, and restore/integrity
  (`rclone copy`/`rclone check`) — completing Epic C (tiered storage). (#91)

### Changed

### Fixed

### Deprecated

### Removed

## [3.0.0] - 2026-06-07

### Added

- Reworked web UI split by concern: a read-only enriched **Inventory** (data
  freshness, OHLC gap detection, on-disk size, per-exchange totals) and two
  collection pages — **Historical** and **Live** — each with data-type tabs and
  per-exchange accordions. Jobs are created, edited (first date) and deleted
  inline on the page; the Live page shows a real-time liveness indicator (last
  trade/quote + age) fed by a throttled stream heartbeat over SSE. (#76)
- Job CRUD over the API: `POST /api/jobs/create|delete|update`, backed by
  `AppConfig.add_job`/`remove_job`/`update_job_start` (persisted to `config.yml`).
- `ParquetStore.inventory()` now reports on-disk `bytes` and, for OHLC,
  `expected_rows`/`missing_rows` (gap detection) at no extra read cost.
- `EventBus` fan-out to multiple SSE consumers and a `StreamSampleEvent`
  liveness sample emitted (throttled) by `operations.stream`.
- UI polish: nav reorganised into `Collect ▾`/`System ▾` dropdowns; **Inventory**
  renamed **Data** (`/inventory`→`/data`) with data-type tabs; reworked Live
  liveness — seeded from the last on-disk data point so a page refresh shows
  freshness immediately (no "waiting…"), span-aware dot, a freshness label that
  is a live relative "N min ago" counter under 24h and an absolute date beyond,
  and no noise age for fresh trades, with client-side number formatting;
  order-book cadence (`snapshot_interval`) shown and settable;
  Storage shows on-disk sizes; Dashboard adds a KPI bar and clearer sections;
  Logs reoriented around recent runs with human run labels. The Config page no
  longer duplicates job management (jobs live on Historical/Live; raw edit via
  its JSON tab). `GET /api/jobs` now returns `start`/`every`/`snapshot_interval`/
  `depth`. (#76)
- Cursor-based trades pagination: the engine now follows each adapter's opaque
  cursor until a window is drained, instead of advancing by a fixed time window.
  Fixes silent loss of >95% of trades on every liquid pair (all exchanges).
- UI: single-line top bar (brand + nav on one row); per-job **Schedule** on
  Historical (a recurring backfill cron — Off/hourly/daily/custom, independent of
  the span but `≥` it), reconciled live via `Scheduler.sync_intervals`; **Run
  all** (global) and per-exchange run; timezone-aware date display driven by
  `settings.timezone` (`local`/`UTC`/zoneinfo). OHLC removed from Live (collected
  via Historical schedule); order books removed from Historical (no REST
  history). `POST /api/jobs/update` now also sets `every` (schedule); new
  `manual` trigger kind for never-auto-run jobs.
- Bearer auth on `/api/*` when `settings.ui_auth_token` is set, with a `?token=`
  fallback for Server-Sent Events; `settings.ui_allow_origins` for opt-in CORS.
- Public async `Client.read()` and `Client.stream()`; `Client` wires adapters
  via `service_factory` (single source of truth).
- Network-marked end-to-end tests (`pytest -m network`) validating pagination
  against live exchange APIs.

### Fixed

- Data loss on merge: writing into an existing legacy v2 Parquet file no longer
  silently overwrites it; existing rows are canonicalised and preserved.
- Provenance is now actually written into the Parquet footer (was computed but
  dropped).
- Custom ISO start date for backfill no longer raises (`JobParams.start`).
- Historical *first date* edit no longer reverts on reload: `GET /api/jobs` was
  not returning `start`, so the UI reset the field after every refresh. (#76)
- Live order-book streams reported a crossed/incorrect best bid-ask: the WS
  adapters emitted unmerged diff levels. binance/okx/bitmex now use full
  snapshot channels (`@depth<N>`, `books5`, `orderBook10`) and bybit
  reconstructs full state from snapshot+deltas (like kraken); best bid/ask is
  computed defensively (`max` bid / `min` ask). (#76)
- Order-book Live liveness was incoherent with its cadence: it sampled the WS
  every second while only one snapshot per ``snapshot_interval`` is captured. The
  liveness sample is now emitted when a snapshot is actually saved, so its age
  counts up to the interval and resets (matching the "Δ Ns" cadence). (#76)
- `dccd inventory` no longer crashes on OHLC datasets.
- Streams with no real implementation (Coinbase OHLC/order book, Bitfinex order
  book) are rejected with `NoCapability` instead of "running" with zero output.
- `history="recent"` exchanges (Kraken OHLC) are clamped + warned instead of
  silently returning wrong deep history.
- Kraken live OHLC timestamps were epoch 0 (1970): the WS adapter read a
  non-existent `timestamp_open`; it now parses `interval_begin` (ISO-8601).
- `mypy dccd/` runs and passes again (it had been aborting on the dev Sphinx).

### Changed / Removed

- Docs/examples swept to v3: README drops the removed `dccd migrate` command and
  the "Migrating from v2" section; `examples/` rewritten to the v3 `Client` and
  `dccd.application` daemon wiring with a v3 `jobs:` config, and the stale v2
  `historical_downloader.ipynb` removed. (#82)
- Honest OHLC fidelity: Coinbase `quote_volume` is null (was a fabricated
  `close×volume`); Kraken now fills its native trade count.
- Removed the dead `parallel` backfill flag, the unused `Page` model and the
  unused bundled `htmx.min.js`.
- Removed the v2→v3 Parquet migration tool entirely: `dccd migrate`,
  `POST /api/migrate`, the Storage-page migrate card, `dccd/storage/migrate.py`,
  and the `migrate` operation in the registry.

> v3 is a full hexagonal rewrite. It **removes** the v2 daemon web UI shipped in
> 2.4.0 (`dccd/daemon/*`) and replaces it with `dccd/interfaces/` (api/cli/ui).

## [2.4.0] - 2026-06-04

### Added

- `dccd/daemon/api.py` — web UI and JSON API (FastAPI + Jinja2 + htmx): a thin
  HTTP layer over the existing daemon modules exposing dashboard (live health
  metrics), inventory (stored data coverage), jobs (histo/stream list + add/remove
  + live backfill progress), logs (tail), config (view/validate/save the YAML),
  and storage (rclone status + manual sync). JSON-only API (`/api/*`) with
  dumb-shell templates, so the front-end can be swapped without touching the API.
  Optional Bearer-token auth via `settings.ui_auth_token`
- `dccd/daemon/cli.py` — `dccd ui`: serve the web UI standalone; the UI is also
  started automatically (background thread) by `dccd start` when the `[ui]` extra
  is installed
- `dccd/daemon/config.py` — `SettingsConfig.ui_host`, `ui_port`, `ui_auth_token`:
  web UI bind address, port, and optional auth token
- `dccd/daemon/backfill.py` — `progress_callback` and `stop_event` on
  `_BackfillBase.run()` / `run_backfill()`: let the UI report live progress and
  cancel a running backfill (defaults keep CLI behaviour unchanged)
- `dccd/daemon/stream_manager.py` — `SyncService` writes
  `{local_path}/.dccd/last_sync.json` after each successful remote push, so the UI
  can display the last sync time
- `pyproject.toml` — new optional extra `[ui]` (`fastapi`, `uvicorn[standard]`,
  `jinja2`); install with `pip install dccd[daemon,ui]`

## [2.3.3] - 2026-05-31

### Added


- `doc/source/` — complete Sphinx documentation overhaul: redesigned homepage
  with sphinx-design cards, captioned toctrees (Getting Started / Data Collection /
  Reference), new pages (`installation`, `quickstart`, `changelog`, `cli`,
  `configuration`, `models`, `storage`, `tools`, per-exchange histo/continuous pages),
  adaptive light/dark logo and favicon, sticky top navbar with PyPI/GitHub/Fynance
  links, hero header with inline logo+title, responsive layout (#59, #61)
- `README.md` — converted from RST to Markdown; inline logo+title header with
  `<picture>` for light/dark mode switching; badges on two rows (#60)

## [2.3.2] - 2026-05-25

### Added

- `dccd/daemon/cli.py` — `dccd status --json`: emit raw metrics as a JSON object on stdout, suitable for piping into Grafana / jq (#53)
- `dccd/daemon/config.py` — `HistoJob.max_retries` (int, 1–10, default 3) and `HistoJob.retry_delay` (float ≥ 0, default 2.0): per-job retry configuration for transient network errors; delay is exponential (`retry_delay * 2^(attempt-1)`) (#53)
- `dccd/daemon/config.py` — `resolve_config_path()` and `DEFAULT_CONFIG_PATH`: CLI commands now fall back to `$XDG_CONFIG_HOME/dccd/config.yml` (default `~/.config/dccd/config.yml`) when no `--config` option is provided and `./config.yml` does not exist (#49)
- `dccd/daemon/cli.py` — `dccd inventory`: scans `data_path` and prints a table of all stored OHLC, trades, and orderbook data with date range, row count, and gap count per series; uses Polars for fast columnar reads (#50)
- `dccd/daemon/cli.py` — `dccd remove --exchange X --pair Y --span N`: removes a pair from a histo_job (or the whole job if it was the last pair) and re-validates the config before writing (#50)

### Changed

- `dccd/storage.py`, `dccd/histo_dl/exchange.py`, `dccd/daemon/backfill.py`, `dccd/process_data.py`, `dccd/daemon/stream_manager.py` — replace pandas with polars throughout; `DataStore.save/load` accept/return `pl.DataFrame`; `get_data()` defaults to `format='polars'`; `set_marketdepth` returns a flat long-format `pl.DataFrame`; stream savers write parquet via `DataStore`; `pandas` removed from core dependencies (#52)
- `dccd/daemon/backfill.py` — backfill progress bar now shows the current window date (`YYYY-MM-DD → YYYY-MM-DD`) instead of a raw window count (`n win`); makes it easy to see which period is being downloaded at a glance (#48)

### Fixed

- `dccd/histo_dl/exchange.py` — `_sort_data` no longer raises `ColumnNotFoundError: "date"` when the exchange API returns an empty candle list; the polars migration (PR #52) had re-introduced a variant of the empty-data crash from v2.3.1; now returns early with an empty `self.df` (#54)

## [2.3.1] - 2026-05-24

### Fixed
- `dccd/storage.py` — `DataStore.missing_intervals` now detects the gap **before** the first saved row when the requested `start` predates `file_min`; previously only the trailing gap (after `file_max`) was returned, causing `dccd backfill --start <early-date>` to silently skip all historical data before the first existing candle (#46)
- `dccd/histo_dl/coinbase.py` — raise `RuntimeError` when Coinbase returns HTTP 200 with a JSON dict (e.g. `{"message": "..."}` for near-future windows) instead of silently iterating dict keys and crashing with `ValueError` (#45)
- `dccd/histo_dl/coinbase.py` — additional guard: raise `RuntimeError` when Coinbase returns a JSON list whose first element is not itself a list/tuple (e.g. `["message"]`); previously caused `float("m")` `ValueError` (#45)
- `dccd/histo_dl/exchange.py` — `_sort_data` no longer raises `KeyError: 'TS'` when the API returns empty data; returns early with an empty `self.df` so the backfill skips the window cleanly (#45)
- `dccd/histo_dl/exchange.py` — `_sort_data` strips any candle at or beyond `self.end` before merging; exchanges with inclusive endpoint semantics (Coinbase) no longer cause `_advance` to overshoot by one span per window, preventing drift that accumulated into near-future requests (#45)
- `dccd/histo_dl/okx.py` — raise `RuntimeError` when OKX response code is not `"0"`, letting the backfill retry/skip logic handle API-level errors (#45)
- `dccd/histo_dl/okx.py` — switch `_import_data` from `/market/candles` to `/market/history-candles`; the former only serves the last ~24 h of 1-minute bars and silently returns empty data for older windows (#45)

## [2.3.0] - 2026-05-22

### Added

- `dccd/storage.py` — `DataStore.is_period_complete(year)`: checks whether an annual parquet file contains all expected candles; `DataStore.missing_intervals(start, end)`: gap-detection — complete past years are skipped, incomplete years resume from the last saved row (#41)
- `dccd/daemon/backfill.py` — `_BackfillBase.run()` now iterates over `DataStore.missing_intervals()` instead of a single sliding window from `last_saved`; complete years are never re-downloaded (#41)
- `dccd/storage.py` — new `DataStore` class: unified read/write interface for OHLC, trades, and orderbook; `save(df)` (merge-on-TS, annual OHLC / daily trades+orderbook), `load(start, end)`, `existing_periods()`, `last_timestamp()` (#39)
- `dccd/tools/date_time.py` — `span_label(span)` converts seconds to short directory labels (``'1m'``, ``'1h'``, ``'1d'``…); `_SPAN_LABEL` mapping exported (#39)
- `doc/source/storage.rst` — Sphinx page for `DataStore` with directory layout examples (#39)

### Changed

- `dccd collect` (formerly `dccd run`) — renamed to clarify the distinction: `collect` = one incremental batch, `backfill` = full historical download with gap detection, `start` = continuous daemon (#41)
- New storage arborescence: ``{data_path}/{exchange}/ohlc/{pair}/{span}/YYYY.parquet``, ``…/trades/{pair}/YYYY-MM-DD.parquet``, ``…/orderbook/{pair}/YYYY-MM-DD.parquet`` — replaces the old ``{Exchange}/Data/Clean_Data/{per}/{pair}/`` layout (#39)
- `dccd/histo_dl/exchange.py` — `save()`, `_get_last_date()`, `save_trades()`, `save_orderbook()` now delegate to `DataStore`; removed `last_df`, `_set_by_period`, `_name_file`, `_excel_format`; removed unused `set_hierarchy()` (#39, #41)
- `dccd/histo_dl/{binance,bybit,coinbase,okx}.py` — removed `full_path` overrides (base class sets the correct path via `DataStore`) (#39)
- `dccd/daemon/backfill.py`, `scheduler.py` — removed `by_period` parameter; `save()` call simplified (#39)
- `dccd/daemon/stream_manager.py` — WebSocket save path now built from `DataStore.directory` (#39)
- `dccd/daemon/config.py` — `HistoJob.by_period` field removed; granularity is automatic (#39)

- `dccd/histo_dl/exchange.py` — `save()` now supports `form='parquet'`; previously only `'xlsx'` and `'csv'` were handled (#35)
- `config.yml` — ready-to-use daemon config for minutely OHLC + real-time orderbook/trades on Binance, Kraken, and Bybit (#35)
- `dccd/daemon/backfill.py` — `OHLCBackfill` and `KrakenBackfill` strategy classes with shared retry/progress/save loop; `make_job()` factory; `run_backfill()` orchestrator; tqdm progress bars and optional `--parallel` execution (#38)
- `dccd/daemon/cli.py` — `dccd backfill` command: reads all job definitions from config, supports `--exchange` / `--pairs` filters, `--start`, `--parallel`, and `--dry-run` flags (#38)
- `dccd/daemon/config.py` — `SettingsConfig` with `data_path` and `timezone` fields; `CollectorConfig.settings` propagates `data_path` to `StorageConfig.local_path` when not set explicitly (#38)

### Removed

- `scripts/backfill.py` — replaced by `dccd backfill` CLI command and `dccd.daemon.backfill` module (#38)

### Fixed

- `dccd/histo_dl/exchange.py` — `save(form='parquet')` was silently ignored (logged a warning instead of writing the file) (#35)
- `dccd/histo_dl/exchange.py` — `_sort_data()` crashed with a ValueError when the API returned fewer candles than the expected window size; index is now derived from actual data (#36)
- `dccd/histo_dl/exchange.py` — `by_period='M'` produced minute-level file names (strftime `%M`) instead of year-month; added `_PERIOD_FMT` mapping so `'M'` → `'%Y-%m'` (#36)
- `dccd/histo_dl/exchange.py` — `self.end` now reflects the last candle timestamp so window-loop callers advance correctly (was stuck at `now` for Kraken) (#36)
- `dccd/histo_dl/binance.py` — missing `limit=1000` parameter caused Binance to return only 500 candles per request (#36)
- `dccd/histo_dl/bybit.py` — `limit` was 200; raised to 1 000 to match the API maximum (#36)
- `dccd/histo_dl/exchange.py` — `_sort_data()` dropped the minute just before a window boundary when the last trade arrived ≥2 spans early; grid now uses `self.end` directly as the exclusive stop (#36)

## [2.2.0] - 2026-05-17

### Added

- `dccd/histo_dl/exchange.py` — `import_trades(start, end)` and `import_orderbook(depth)` public methods on `ImportDataCryptoCurrencies`; `_sort_trades` / `_sort_orderbook` helpers validate via Pydantic, sort and deduplicate; `trades_df` / `orderbook_df` attributes; `save_trades` / `save_orderbook` save helpers (#31)
- `dccd/histo_dl/{binance,kraken,bybit,okx,coinbase}.py` — `_import_trades(start, end)` and `_import_orderbook(depth)` implemented for all five exchanges; Binance and Kraken support full history via paginated endpoints; Bybit (≤ 1 000) and Coinbase (≤ 100) return recent-only snapshots (#31)
- `dccd/models.py` — `Trade.tid` made optional (`int | None`); `OrderBookEntry` gains required `side` field (`'bid'` or `'ask'`) and `count` made optional (`int | None`) (#31)
- `dccd/daemon/health.py` — `HealthMonitor`: rotating log handler (10 MB × 5 files), per-job metrics JSON, and optional Slack/Discord webhook alerts on consecutive failures; `JobMetrics` dataclass (#30)
- `dccd/daemon/cli.py` — `dccd` CLI (`validate`, `run`, `start`, `status`, `add` commands) via typer; `[project.scripts]` entrypoint; `typer>=0.12` added to the `daemon` extra (#30)
- `dccd/daemon/stream_manager.py` — `StreamManager` (one thread per `(exchange, pair)`, auto-restart on crash) and `SyncService` (periodic rclone push to all remotes, decoupled from collection) (#26)
- `dccd/daemon/config.py` — declarative YAML config with Pydantic v2: `CollectorConfig`, `HistoJob`, `StreamJob`, `StorageConfig`, `AlertConfig`, `RemoteConfig`, `load_config()` (#25)
- `dccd/daemon/storage.py` — `RemoteStorage.push()` via rclone; supports multiple remotes and root-path sync (#25, #26)
- `dccd/daemon/scheduler.py` — `build_histo_scheduler()` (APScheduler 3.x), `run_histo_job()`, `run_once()` (#25)
- `examples/config.example.yml` — annotated reference config for the daemon (#25)
- `examples/daemon_example.py` — programmatic daemon example in 6 steps (#30)
- `pyproject.toml` — `[daemon]` optional extra (`pyyaml`, `apscheduler`, `typer`) (#25, #30)

### Changed

- `dccd/daemon/scheduler.py` — `run_histo_job`, `build_histo_scheduler`, `run_once` accept an optional `health: HealthMonitor` parameter (#30)
- `dccd/daemon/stream_manager.py` — `StreamManager.__init__` accepts optional `health: HealthMonitor`; `_run_forever` records success/failure on each iteration (#30)
- `dccd/daemon/config.py` — `StorageConfig.remote` replaced by `remotes: list[RemoteConfig]` and `sync_interval: int` (#26)
- `dccd/histo_dl/{binance,coinbase,bybit,okx,kraken}.py` — `format_pair(crypto, fiat)` extracted as a static method, independently testable (#29)
- `dccd/continuous_dl/exchange.py` — unified `__call__`, `_push_trades`, `_push_book_updates`, `_get_book_state`, `_restore_book_state` in base class; separate `set_trades_saver` / `set_book_saver`; crash-recovery checkpoint; `snapshot_ts` injected into every snapshot payload (#28, #29)

## [2.1.0] - 2026-05-15

### Added

- `dccd/tests/test_binance.py`, `test_kraken.py`, `test_bybit.py`, `test_okx.py`, `test_coinbase.py` — REST error-scenario tests: HTTP 500 and malformed response for every exchange (#22)
- `dccd/continuous_dl/binance.py` — `DownloadBinanceData` streaming trades and order book via Binance combined WebSocket streams (#20)
- `dccd/continuous_dl/kraken.py` — `DownloadKrakenData` streaming trades, order book, and OHLCV via Kraken WebSocket v2 (#20)
- `dccd/continuous_dl/okx.py` — `DownloadOKXData` streaming trades, order book, and candles via OKX WebSocket v5 (#20)
- `get_trades_*`, `get_orderbook_*`, `get_data_*` high-level helpers for Binance, Kraken, and OKX (#20)
- `dccd/tests/test_binance_ws.py`, `test_kraken_ws.py`, `test_okx_ws.py` — 34 new tests for the new WS modules (#20)
- `README.rst` and `doc/source/index.rst` — exchange support matrix table (REST/WS × data type) (#20)
- `dccd/tests/test_websocket.py`, `test_bitfinex.py`, `test_bitmex.py`, `test_bybit_ws.py` — tests for `continuous_dl` and `BasisWebSocket`; coverage lifted from excluded to 82% overall (#12)
- `dccd/tests/test_histo_dl.py` — tests for `_get_last_date` (xlsx, csv, parquet, empty directory) (#12)
- `pyproject.toml` — `pytest-asyncio>=0.23` added to dev dependencies (#12)

### Fixed

- `dccd/tools/date_time.py` — `span_to_str` and `str_to_span` now cover all spans supported by the exchanges: 180 s (3 m), 900 s (15 m), 14400 s (4 h), 21600 s (6 h), 28800 s (8 h), 43200 s (12 h), 259200 s (3 d), 1296000 s (15 d), 2592000 s (1 M); previously any span outside the original 7 values returned `None` and silently broke the save path (#21)
- `dccd/histo_dl/kraken.py` — `import_data` now raises `UserWarning` when `end` is passed, as the Kraken OHLC API does not support a custom end date and silently ignored the parameter (#21)

### Changed

- `dccd/continuous_dl/exchange.py` — `get_parser()` now raises `KeyError` on unknown key instead of falling back to the removed debug parser; `_loop()` awaits `is_connect` instead of `_data` (#22)
- `dccd/continuous_dl/bitmex.py`, `dccd/continuous_dl/bybit.py` — added numpydoc docstrings on `_parser_trades()` and `_parser_book()` (#22)
- `dccd/histo_dl/exchange.py` — `ImportDataCryptoCurrencies` docstring updated: `See Also` lists all five exchanges; `platform` parameter documents all supported values; fixed typos (#22)
- `dccd/histo_dl/exchange.py` — `ImportDataCryptoCurrencies` now inherits from `ABC` and `_import_data` is decorated with `@abstractmethod`, preventing accidental instantiation of the base class (#21)
- `dccd/histo_dl/binance.py`, `coinbase.py`, `bybit.py`, `okx.py`, `kraken.py` — added `from __future__ import annotations`, `from typing import Any`, and full type hints on `_import_data` and `import_data` signatures (#21)
- `dccd/histo_dl/exchange.py` — `_get_last_date` now reads `.csv` and `.parquet` files in addition to `.xlsx` instead of falling back to 2012-01-01 (#12)
- `dccd/histo_dl/exchange.py` — completed numpydoc docstrings for `_get_last_date`, `_set_by_period`, `_name_file`, `_excel_format`, `_sort_data`, `set_hierarchy` (#12)
- `dccd/tools/io.py` — documented `driver`, `username`, `password`, `host`, `port` parameters of `save_as_sql` (#12)
- `dccd/continuous_dl/exchange.py` — documented `time_step=None` tick-by-tick behaviour in `ContinuousDownloader` (#12)
- `dccd/continuous_dl/bitfinex.py` — resolved all inline TODOs, added full type annotations, removed dead `__main__` block (#12)
- `dccd/continuous_dl/bitmex.py` — resolved all inline TODOs, added full type annotations, fixed undefined `pair` variable in `get_data_bitmex`, removed dead `__main__` block (#12)
- `pyproject.toml` — removed `bitfinex` and `bitmex` from mypy `ignore_errors` override; lifted `continuous_dl/*` and `tools/websocket.py` from coverage omit (#12)

### Removed

- `ContinuousDownloader._parser_debug()` — dead method, never called; `dccd/tools/__init__.py` commented-out imports removed (#22)

## [2.0.2] - 2026-05-15

### Changed

- `README.rst` — added PyPI status, docstring coverage, and downloads badges

## [2.0.1] - 2026-05-14

### Changed

- Docstrings `See Also` updated in `FromBinance`, `FromKraken`, `FromCoinbase` — replaced defunct `FromGDax`/`FromPoloniex` with `FromBybit`/`FromOKX`
- `doc/source/index.rst` — exchange lists updated (Bybit, OKX added); all exchange RST pages added to toctree (previously orphaned)
- `dccd/__init__.py` module docstring — exchange list updated
- `pyproject.toml` — added `Documentation` and `Changelog` project URLs
- `README.rst` — added exchange support table, output format section, multi-exchange Quick start examples
- `examples/historical_downloader.py` — rewritten with modern API (Binance + Parquet)

## [2.0.0] - 2026-05-14

### Added

- `pyproject.toml` (PEP 517/518) — replaces `setup.py` (#5)
- GitHub Actions CI (`.github/workflows/ci.yml`) — matrix Python 3.10/3.11/3.12/3.13, jobs `test` and `lint` (#5)
- `dccd/histo_dl/coinbase.py` — `FromCoinbase` class replacing the defunct GDAX module (#5)
- `.githooks/pre-push` — Git Flow enforcement (#5)
- `CONTRIBUTING.md` — development setup, Git Flow, commit conventions (#5)
- `CHANGELOG.md` (#5)
- `.pre-commit-config.yaml` — hooks `ruff` (lint + fix) et `ruff-format` (#7)
- `dccd/tests/test_date_time.py`, `test_io.py`, `test_process_data.py` — couverture ≥ 80 % (#8)
- `.github/workflows/badges.yml` — badge couverture docstrings via `interrogate` (#8)
- `dccd/histo_dl/bybit.py` — `FromBybit` : téléchargement historique Bybit v5 REST (#9)
- `dccd/continuous_dl/bybit.py` — `DownloadBybitData` : stream WebSocket Bybit v5 (#9)
- `dccd/histo_dl/okx.py` — `FromOKX` : téléchargement historique OKX v5 REST (#9)
- `dccd/models.py` — `OHLCBar`, `Trade`, `OrderBookEntry` : validation pydantic des réponses API (#9)
- `IODataBase.save_as_parquet` — format Parquet via pyarrow (optionnel `dccd[io]`) (#9)
- `IODataBase.save_as_polars` — format Polars, Parquet sous le capot (optionnel `dccd[io]`) (#9)
- `ImportDataCryptoCurrencies.get_data(format='polars')` — retourne un `pl.DataFrame` (#9)
- `dccd/tools/date_time.py`, `tools/io.py`, `histo_dl/exchange.py`, `continuous_dl/exchange.py`, `tools/websocket.py` — type hints complets (#10)
- `.github/workflows/release.yml` — publication automatique PyPI + GitHub Release sur tag `v*` via OIDC (#10)

### Changed

- **Breaking:** minimum Python version is now 3.10 (dropped 3.5–3.9) (#5)
- **Breaking:** minimum dependency versions bumped — `pandas>=2.0`, `SQLAlchemy>=2.0`, `numpy>=1.26`, `requests>=2.28`, `websockets>=12.0`, `scipy>=1.10` (#5)
- Replaced `xlrd` + `xlsxwriter` with `openpyxl` for Excel I/O (#5)
- `dccd/histo_dl/exchange.py`: `df.append()` → `pd.concat()`, `ffill()`, `openpyxl` engine (#5)
- `dccd/tools/io.py`: `SQLAlchemy URL()` → `URL.create()`, `df.append()` → `pd.concat()` (#5)
- Version now managed via `importlib.metadata` (#5)
- `dccd/tools/websocket.py`: `asyncio.get_event_loop().run_until_complete()` → `asyncio.run()` (#7)
- `dccd/tests/conftest.py`: fixtures `tmp_data_path` + mocks HTTP — les tests ne font plus d'appels réseau (#7)
- `doc/source/conf.py` : thème scipy → furo, extensions modernisées (#8)
- `dccd/histo_dl/binance.py` : API v1 → v3 (#9)
- `dccd/histo_dl/exchange.py` : `_fetch()` avec retry tenacity sur HTTP 429 (#9)
- `dccd/tools/websocket.py` : reconnexion automatique avec `max_retries` et `retry_delay` (#9)
- `print()` remplacés par `logging` dans `exchange.py`, `binance.py`, `date_time.py` (#9)
- `pyproject.toml` : `mypy>=1.0` + `pandas-stubs>=2.0` dans `dev`, section `[tool.mypy]` (#10)
- `.github/workflows/ci.yml` : étape `mypy dccd/` ajoutée dans le job `lint` (#10)
- `dccd/tools/websocket.py` : arguments mutables `conn={}` / `subs={}` corrigés en `None` (#10)

### Fixed

- `dccd/tools/io.py` : logique CSV inversée dans `save_as_csv` — le fichier existant était écrasé au lieu d'être appendé (#8)

### Removed

- **Breaking:** `FromPoloniex` and `dccd/histo_dl/poloniex.py` — Poloniex exchange shut down in 2024 (#5)
- **Breaking:** `FromGDax` and `dccd/histo_dl/gdax.py` — GDAX API endpoint defunct; replaced by `FromCoinbase` (#5)
- `setup.py`, `tox.ini`, `requirements.txt`, `doc-requirements.txt`, `.travis.yml` (#5)
