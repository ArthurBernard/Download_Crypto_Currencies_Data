# 3 — Design decisions & rationale

The *why* behind the structure. Each entry is a choice that shapes the code; if
you're about to change one, read the rationale first.

## Core architecture

- **Hexagonal layers.** The domain is pure and synchronous so logic is trivially
  testable and I/O is swappable. The v2 codebase mixed I/O with logic; v3 inverts
  the dependencies. Cost: more indirection (`service_factory` wiring) — accepted
  for testability and the 3-interface parity.
- **One wiring source (`service_factory`).** `Client`, CLI and API must build the
  exact same registry/store, or the three modes drift. Never re-wire adapters ad
  hoc — call the factory.
- **Nanosecond UTC `int64` everywhere internal.** v2 had heterogeneous time
  units; ns int64 is exact, sortable, and dedup-friendly. Conversions happen only
  at the edges (adapter parse, UI display).

## Collection engine

- **Capability-driven, not exchange-special-cased.** Adapters *declare*
  `history`, `max_per_request`, `page_direction`, `spans`, `max_depth`; the engine
  reads them. This was the central remediation: the first v3 engine ignored
  capabilities and paginated every trade feed by a fixed time window, silently
  losing >95% of trades on 5/7 exchanges. The fix generalised pagination instead
  of patching each exchange.
- **Trades pagination is cursor-based.** Each adapter returns `(items,
  next_cursor)` (opaque: `fromId`, `since`-ts, `after`-ts, …); the paginator
  follows the cursor until the window drains. A fixed window can't work — page
  caps range from 60 (Bybit) to 10 000 (Bitfinex).
- **Honest capabilities + early `NoCapability`.** Better to reject Bybit *spot
  trades history* up front than to "run" and produce zero rows. A WS channel that
  isn't really implemented must not be declared (the silent-empty-stream bug).
- **Bounded first backfill.** A first `start=last` on an empty dataset uses a
  per-type default lookback (short for trades), so one click can't trigger a
  multi-million-row run from epoch 0.

## Storage

- **Per-data-type dedup keys.** OHLC=`TS`; trades=`tid` (else a composite);
  order book=`(TS, side, price)`. Using `TS` alone for trades collapsed 58% of
  rows in an early bug — `TS` is unique only for OHLC.
- **Defensive reads + provenance.** Legacy v2 frames are canonicalised (columns
  renamed/aligned) before any `concat`, so a stale file can never be silently
  overwritten or lose rows. Provenance is written into the Parquet footer (it was
  computed-but-dropped early on).
- **Annual OHLC files, daily trades files.** Matches access patterns and keeps
  files a sane size.

## Events & streaming

- **SSE, not browser WebSocket.** The browser only *receives* updates
  (server→client). SSE is one-way, auto-reconnecting (`EventSource`), works over
  plain HTTP, and supports `?token=` auth. A browser WS would add a bidirectional
  channel, new auth, and manual reconnect for zero benefit. (The WS *to exchanges*
  is a separate concern, handled by `transport/ws.py`.)
- **EventBus multi-queue fan-out.** Each SSE consumer (Live + Logs + Dashboard in
  separate tabs) registers its own queue; a single shared queue let the last
  connection steal events.
- **Liveness via a throttled `StreamSampleEvent`** (≤1/s), carrying *numeric*
  `value`/`bid`/`ask` (the client formats). Samples are **not persisted**.
- **Order-book liveness is tied to the snapshot save, not the WS frame.** The WS
  pushes the book continuously but only one snapshot per `snapshot_interval` is
  captured; emitting the sample on save keeps the liveness coherent with what's
  actually collected (the age counts up to the interval and resets).

## Web UI model (post-rework, PR #76)

- **Split by concern.** Read-only **Data** (what's on disk), **Historical**
  (backfill jobs), **Live** (streams) — instead of one page that both browsed and
  launched. Each is data-type tabs → per-exchange accordions → one row per
  dataset, with inline job create/edit/delete (no Config detour).
- **Config no longer manages jobs.** Jobs live on Historical/Live; the Config
  page keeps Settings/Alerts/Storage + a raw-JSON tab for bulk edits (it preserves
  the `jobs` array on save). This removed a duplicated job form.
- **Liveness freshness label rule.** A live relative "N ago" counter under 24h,
  an absolute date beyond; the last-run date-time when stopped. The dot's "fresh"
  window is span-aware (OHLC span / order-book `snapshot_interval` / short for
  trades) so a slow feed doesn't read "dead".
- **Two dropdown nav groups** (`Collect ▾`, `System ▾`) with `Dashboard`/`Data`
  flat — chosen over flat-7 or inline group labels.

## History in two paragraphs

v3 was a from-scratch hexagonal rewrite (phases P0–P8). The architecture landed
well, but the first cut shipped with data-correctness regressions that unit tests
missed: a GC'd backfill task writing 0 rows, trades pagination losing >95% of
data, dedup collapsing trades, provenance never written, the engine ignoring
declared capabilities, and a "Stop" button that did nothing. A remediation pass
fixed these, added cursor pagination, a one-shot v2→v3 migration (run and
verified on the existing ~120 files, zero loss, since removed), Bearer auth, and
network-marked E2E tests.

The recurring lesson — now baked into how we test — is **challenge every result
on real data**: a green unit suite said nothing about a backfill that wrote 0
rows or a store that lost half its trades. See `05-testing.md`. The most recent
round (the UI rework + order-book liveness fixes) followed the same method:
drive the real UI, read what renders, compare to the feed.

## Decision journal (ADR)

Append-only, dated log of choices made since the v3 brief — fed by `/finish-task`
(accepted) and `/abandon-task` (rejected / tombstone). The prose above is the
*settled* rationale; this journal is the *running* one. **Newest first.**

Conventions:
- One entry per significant choice; skip the trivial (those live in
  git/`CHANGELOG.md`).
- `[tombstone]` = a feature was removed. Keep one line on *why it's gone* here and
  **purge its implementation rationale from the prose above** — negative knowledge
  so it isn't silently re-added later.

Template:
```
### YYYY-MM-DD — <short title> (PR #94)  [accepted|rejected|tombstone]
- **Choice**: …
- **Why**: …
- **Rejected alternatives**: …
```

<!-- new entries below, newest first -->

### 2026-07-03 — Market lives on `Symbol`, honesty via `Capability.markets` (PR #183)  [accepted]
- **Choice**: derivative addressing is a `Symbol.market` literal
  (`spot|perp|quarter|next_quarter`, default `spot`) with a `:market` string
  suffix, not a new `DatasetId` field or a separate symbol type. Capabilities
  declare supported markets via `markets: list[str] | None` (`None` = spot-only)
  plus `recent_window_s` for time-bound recent windows; `backfill()` rejects an
  undeclared market (`_check_market`) before any fetch.
- **Why**: market-on-Symbol flows through job ids (`str(symbol)`), storage slugs
  (`pair_slug()` suffix) and adapters for free — one field, zero schema
  migration, spot behaviour bit-for-bit unchanged. `markets=None` keeps every
  existing declaration honest without touching the seven adapters.
- **Rejected alternatives**: `DatasetId.market` (wouldn't reach adapters or job
  ids without duplicating the field); a separate `PerpSymbol` type (fans out
  through every signature); encoding the market in the quote (`USDT-PERP` —
  collides with real tickers like PERP and breaks alias normalisation).

### 2026-06-20 — Null the shared HTTP client before awaiting `aclose()` (PR #173)  [accepted]
- **Choice**: in `AsyncHTTPClient.__aexit__`, when the ref-count hits zero, capture
  the client into a local, set `self._client = None` and `self._depth = 0`, **then**
  `await client.aclose()` — null-before-await rather than the previous
  null-after-await.
- **Why**: `aclose()` awaits (yields control). With the old ordering, a concurrent
  `__aenter__` during that window saw the still-set, closing client, skipped
  creation, bumped the depth and issued a request on a dead pool → `Cannot send a
  request, as the client has been closed` (~1 scheduled backfill/day on the server,
  self-healing via the 3600 s backoff). Nulling first makes the re-entrant caller
  build a fresh client, so the closing one is never reused. Refines the ref-count
  pool from [2026-06-11 — HTTP pool lifetime = operation scope (PR #129)]; the
  invariant "shared reference-counted HTTP client (concurrency-safe)" had this hole.
- **Rejected alternatives**: an `asyncio.Lock` around enter/exit (heavier, and the
  only contended region is the single `aclose()` await — a lock would serialise all
  enters for no extra safety); never closing the client / keep-alive for daemon
  lifetime (leaks a pool when the daemon idles and contradicts the operation-scope
  decision of #129).

### 2026-06-20 — Map Kraken pairs by altname, not legacy X/Z codes (PR #169)  [accepted]
- **Choice**: `_kraken_pair` builds the Kraken **altname** (`{base}{quote}` with a
  small alias map `BTC→XBT`, `DOGE→XDG`) instead of the legacy prefixed codes
  (`X{base}Z{quote}` / `X{base}X{quote}`).
- **Why**: the legacy prefix convention only applies to Kraken's *old* assets;
  modern listings (TRX, DOT, BNB, …) have no prefix and Dogecoin's code is `XDG`,
  so the old formula produced `Unknown asset pair`. Kraken accepts altnames for
  *every* asset, and both `fetch_ohlc_page` and `fetch_trades_page` already parse
  the response with a code-key fallback (Kraken keys results by internal code, not
  the altname sent), so the request form can change without touching parsing.
  Verified live across legacy (incl. the EUR pairs the server already collects)
  and modern pairs.
- **Rejected alternatives**: (a) a hardcoded full asset→code map — brittle, needs
  upkeep per listing; (b) a runtime `AssetPairs` lookup at adapter init — an extra
  network call and cache for no benefit, since altnames already work universally.

### 2026-06-19 — Reject invalid timestamps (TS<=0) at the storage write boundary (PR #165)  [accepted]
- **Choice**: guard against corrupt timestamps **centrally in `ParquetStore.save()`**,
  filtering out rows whose `TS` is null or `<= 0` for every data type, rather than
  validating per-adapter. Drop the rows and log a warning — do not raise.
- **Why**: `save()` is the single choke point every source's records flow through,
  so one filter defends all exchanges and all data types at once. `TS` is ns UTC
  int64 and real crypto history starts ~2009, so epoch-or-earlier is unambiguously
  corrupt. Dropping (not raising) means one bad bar never aborts an otherwise-good
  page write. Surfaced by the 2026-06-19 server audit: a Kraken OHLC bar with a
  null-parsed time (`TS=0`) had been written and poisoned gap detection (a stray
  `1970.parquet` made `inventory()` report ~89 % missing on that dataset; store
  OHLC aggregate 32.7 % vs ~1.5 % real).
- **Rejected alternatives**: (a) a Kraken-adapter-only guard — narrower, wouldn't
  protect other adapters or a future regression; (b) filtering in `canonicalize()`
  — that runs on reads/merges too and would silently rewrite history on read,
  conflating ingestion validation with read normalisation; (c) raising on a bad
  bar — would let one corrupt row abort a whole page of good data.

### 2026-06-13 — Dashboard triages candle-coverage gaps; revises the scope of #132 (PR #157)  [accepted]
- **Choice**: the health-first Dashboard *does* alarm on OHLC `missing_rows>0`
  — a "with gaps" health chip and a **Needs attention** item with a one-click
  Retry/Fill action. #132 still governs the **Data** page (neutral "candle
  coverage", no threshold) and the API fields; this entry narrows #132's "no
  alarm *anywhere*" to "no alarm on the neutral *browse* surface".
- **Why**: the two surfaces have different jobs. Data answers "what's on disk?"
  (browse — must not cry wolf on a sparse-but-complete pair); the Dashboard
  answers "what should I act on?" (triage — an operator watching 50 jobs wants
  gaps pulled to the top). Splitting them lets each be honest about its purpose
  instead of forcing one neutral rendering everywhere. The action is
  non-destructive (a bounded backfill re-run), so acting on a false positive is
  cheap, which is what makes the alarm acceptable here but not on Data.
- **Known limit (inherited from #132)**: footer stats alone can't tell "no
  empty candle emitted" from "collection hole", so an illiquid pair *will*
  occasionally surface as a Dashboard "gap". Accepted with eyes open — the
  operator explicitly wanted the signal over the silence.
- **Rejected alternatives**: keep the Dashboard neutral too (defeats the
  triage value the redesign exists for); a warn-below-X% threshold on Data
  (the false-alarm-by-construction #132 already rejected); liquidity-aware
  expectations from trade data (per-row I/O, breaks the footer-stats
  constraint from #119 — same reason #132 rejected it).

### 2026-06-12 — Remote is an archive superset, not a mirror (PR #152)  [accepted]
- **Choice**: `RemoteStorage.sync_one` uploads with `rclone copy` (add/update
  only) instead of `rclone sync` (mirror that deletes remote extras).
- **Why**: the Epic C tiered-storage contract — free-space purge deletes old
  *local* files and read-through restore pulls them back from the remote —
  silently relied on the remote retaining what local drops. With `rclone
  sync`, the hourly cycle after a purge would delete the only remaining copy.
  Latent while `min_free_gb` was 0.0, but the production store now carries the
  full 2020→present history, so the failure mode became "lose the archive".
  Copy semantics make the invariant structural: local = hot tier, remote =
  complete monotonic archive.
- **Rejected alternatives**: `rclone sync --backup-dir` (purged files land in
  a side path; restore would need a second lookup and the layout contract
  breaks); purge-aware exclude lists fed to sync (stateful, fragile, easy to
  desynchronise from what purge actually deleted). Accepted trade-off:
  deleting data from the remote is now a deliberate manual operation.

### 2026-06-11 — Sphinx -W in CI, with docutils warnings suppressed (PR #133)  [accepted]
- **Choice**: a separate CI `docs` job builds with `sphinx-build -W`
  (warnings = errors), and `conf.py` adds
  `suppress_warnings = ['ref.citation', 'docutils']` to mute docutils noise
  coming from the sphinx-click/Typer rendering that we don't control.
- **Why**: the "0 warnings" rule was only enforced by hand; `-W` makes it a
  PR gate (verified to fail on an injected broken reference). The docutils
  suppression is the price of `-W` viability — without it, third-party
  rendering noise would block every PR for warnings we can't fix.
- **Rejected alternatives**: grep-counting warnings in CI (fragile, doesn't
  fail the build atomically); pinning/patching sphinx-click (maintenance
  burden out of proportion).

### 2026-06-11 — Gap metric fix is presentational, not structural (PR #132)  [accepted]
- **Choice**: keep `expected_rows`/`missing_rows` semantics (clock-time slots
  from footer stats) and fix the *presentation*: the Data page shows neutral
  "candle coverage" with a tooltip instead of a red "missing %"; no alarm
  threshold at all.
- **Why**: with footer stats only (min/max/rowcount — the zero-extra-I/O
  constraint from #119) there is no way to distinguish "exchange emits no
  empty candles" from a collection hole; any styling threshold would still
  false-alarm on sparse-but-complete pairs. The audit explicitly allowed the
  labeling fix. True holes still surface as a visibly lower number (verified
  by injecting one).
- **Rejected alternatives**: deriving liquidity-aware expectations from trade
  data (per-row I/O, breaks the footer-stats constraint); a warn-below-X%
  threshold (false-alarms quiet pairs by construction).

### 2026-06-11 — RateLimiter wired as a process-wide per-exchange singleton (PR #130)  [accepted]
- **Choice**: keep `transport/ratelimit.py` and make it real — a
  `shared_limiter()` singleton keyed by exchange, awaited by
  `AsyncHTTPClient.get()` before every outbound request (adapters get it via
  `default_http_client(exchange)` in `sources/base.py`). Conservative
  doc-sourced defaults (kraken 1/s, coinbase 3/s, okx 8/s, bitmex 0.5/s, …);
  reactive 429/Retry-After stays as backstop. Clock/sleep seams injected for
  deterministic tests.
- **Why**: production evidence settled the wire-or-delete question — 481
  `Rate-limited` (429) events in 14 days on arthurserver (OKX 454, Coinbase
  27, single burst day), and the old hardcoded Coinbase rate (10/s) was
  simply wrong (public cap 3/s). The limiter must be process-wide: each
  adapter owns its own HTTP client, so a per-client bucket cannot coordinate
  a `run-all` burst. Verified live: 3 concurrent Kraken backfills share one
  bucket (1.10 req/s total vs the 1.0/s cap), zero 429.
- **Rejected alternatives**: deleting the module and staying reactive-only
  (the journalctl data shows reactive-only thrashing OKX in 2 s retry loops
  for ~90 s); per-client limiters (no cross-operation coordination — the
  exact failure mode observed); weight-based limiting à la Binance (real
  fidelity would need per-endpoint weights — out of scope, conservative
  flat rates suffice).

### 2026-06-11 — HTTP pool lifetime = operation scope (ref-count held), not a keep-alive (PR #129)  [accepted]
- **Choice**: `backfill()` enters the adapter's ref-counted `AsyncHTTPClient`
  once for the whole paginated operation (per-page `async with` becomes a
  ref-count bump); `Client.__aenter__/__aexit__` does the same for every
  REST adapter over the public-API block. Pool lifetime is owned by explicit
  scopes, nothing else.
- **Why**: audit P1 — the ref-count fell 0→1→0 on every page, i.e. one TCP
  pool + TLS handshake *per page* (a 500-page backfill = 500 handshakes),
  and B4 — `Client.__aexit__` was `pass` while its docstring promised
  cleanup ("Cannot send a request, as the client has been closed" seen twice
  in prod 3.3.x).
- **Rejected alternatives**: a grace-period keep-alive on the client (timer
  state + a background reaper for the same effect, and the pool would
  outlive the operation unpredictably); constructing one global pool at
  import (no clean shutdown path for short-lived CLI invocations).

### 2026-06-11 — Stream flush is arrival-driven, not a background task (PR #128)  [accepted]
- **Choice**: streams flush on record arrival when the batch hits 1000 rows
  *or* 60 s elapsed (`_STREAM_FLUSH_INTERVAL_S`) — no separate flusher task.
  `rows_written` is accumulated from every save and reported on all finish
  paths.
- **Why**: with zero arrivals there is nothing in RAM, so an arrival-driven
  check bounds crash loss to one interval plus one inter-record gap — same
  guarantee as a background task without owning another task lifecycle in
  `stream()` (cancellation, exception routing through the existing
  `finish_run` paths).
- **Rejected alternatives**: an `asyncio` background flusher per stream
  (flushes an empty buffer on quiet pairs, adds a second cancellation path
  for no stronger bound); `asyncio.timeout` around the iterator (restructures
  the three loops for the same result).

### 2026-06-11 — Stream supervisor distinguishes permanent from transient errors (PR #126)  [accepted]
- **Choice**: `NoCapability` is treated as *permanent* by `_StreamWorker`:
  the worker logs once, emits `status=failed`, and stops — no retry. All
  other stream exceptions remain *transient* (exponential 5→60 s reconnect),
  and the backoff resets to 5 s when the failed run had been healthy for
  ≥ 300 s. The capability check in `operations.stream()` moved before
  `create_run` so a rejected stream never creates a run row.
- **Why**: prod audit 2026-06-10 (B6) found ~350 zombie `running` rows from
  `stream:bitfinex:*:orderbook` — one per 60 s retry of an error that can
  never succeed; and after weeks of occasional blips every reconnect waited
  the full 60 s. Retrying a misconfiguration is noise, not resilience.
- **Rejected alternatives**: keeping the check after `create_run` and
  finishing the run as `failed` on each attempt (still one row per retry,
  DB churn for a config error); a generic "max retries then give up" cap
  (would also abandon genuinely transient WS outages, e.g. an exchange
  maintenance window).

### 2026-06-10 — Order-book depths declared per capability; invalid requests snap with a warning (PR #122) [accepted]
- **Choice**: `Capability.depths` lists the discrete depths a WS book channel
  accepts (Kraken verified live: {10, 25, 100, 500, 1000}; Bybit spot
  {1, 50, 200, 1000} per v5 docs; Binance {5, 10, 20}; OKX books5 = 5; BitMEX
  orderBook10 = 10). `operations.stream` snaps an undeclared request to the
  smallest valid depth ≥ requested (else the largest) and logs a warning.
  WS subscription rejections now raise from the adapters instead of being
  filtered with the other non-data frames.
- **Why**: the production config had Kraken jobs at depth 20/50 — silently
  rejected, leaving "live" streams that never wrote a row. Honesty needs both
  halves: the engine must know what's valid (capability) *and* the adapter must
  scream when the exchange says no (a raise reaches `_StreamWorker`, the run is
  recorded `failed` with the exchange's own error text).
- **Rejected alternatives**: hard-fail on an invalid depth (existing deployed
  configs — including the production one — must keep collecting after upgrade;
  the warning preserves honesty); validating depth in `AppConfig` (the config
  layer doesn't know per-exchange capabilities — that knowledge lives in
  `sources/`, mirroring how spans are checked at run time).

### 2026-06-10 — Order-book capture throttle lives in the adapter, not the consumer (PR #120) [accepted]
- **Choice**: `OrderBookLive.stream_orderbook` takes a keyword-only
  `min_interval` (default `0.0` = per-frame, the legacy contract).
  Delta-maintained books (Kraken, Bybit) apply frames to plain dicts and only
  sort/construct pydantic objects when a capture is due, truncating snapshot
  *and* state to the subscribed depth; push-snapshot channels (Binance, OKX,
  BitMEX) drop frames before parsing. `operations.stream()` passes
  `snapshot_interval` down and saves every yielded snapshot.
- **Why**: the cost to kill was the *construction* (pydantic `__init__` was
  ~96 % of daemon CPU samples in production), and only the adapter can skip
  it — a downstream throttle (the previous design) pays full price for frames
  it then discards. Default `0.0` keeps the protocol honest for any consumer
  that genuinely wants every frame.
- **Rejected alternatives**: throttle in `operations.stream()` only (where it
  was — provably insufficient, 97.7 % CPU with 20 book jobs); yielding raw
  dict state and building snapshots in the consumer (leaks adapter
  representation across the protocol boundary); plain-dataclass order-book
  records (still O(levels) per frame, and gives up validation everywhere else).

### 2026-06-10 — Store metadata = parquet footer statistics + per-file mtime cache (PR #119) [accepted]
- **Choice**: `ParquetStore` derives rows/min/max TS from parquet **footer
  metadata** (row-group statistics) instead of reading the TS column, cached per
  file on `(mtime_ns, size)`; files without TS statistics fall back to the column
  read. API endpoints call `inventory()` via `asyncio.to_thread`.
- **Why**: inventory was O(total rows) and ran *in the event loop* — the
  production collector served `/api/inventory` in 100 s for 10 KB while WS
  collection shared the same starved loop. Footer stats are O(files), exact for
  int64, and the atomic-rename write path makes mtime+size a sound invalidation
  key. No new state to keep consistent.
- **Rejected alternatives**: a write-through cache updated by `save()` (a second
  source of truth that desyncs on out-of-band writes — purge, rclone restore);
  a manifest DB (CoverageStore already exists for *extent*, duplicating rows/
  bounds there couples two stores for one read path); keeping the column read
  but only off-thread (still O(rows) per call — 365 daily trades files/year/pair
  keeps growing).

### 2026-06-10 — API hardening = in-process rate limit + read-only verb gate, both opt-in (PR #108) [accepted]
- **Choice**: harden `/api/*` for exposure with an in-process, non-blocking per-client
  token bucket (`ui_rate_limit`, over budget → `429`+`Retry-After`) and a read-only
  mode (`ui_readonly`) that blocks mutating verbs (`403`). The rate-limit client key is
  the socket peer unless `ui_trusted_proxy` is set (then `X-Forwarded-For` first hop).
  Layered before/after auth so a `401` still wins over `403` for unauth mutating calls.
- **Why**: a remotely reachable API needs abuse resistance and a safe view-only share
  without standing up Redis/an external WAF. Trusting `X-Forwarded-For` blindly would
  let a direct client forge the key, so it is gated behind an explicit proxy-trust flag.
- **Rejected alternatives**: the blocking `transport.ratelimit` bucket (it sleeps — for
  outbound calls, not inbound rejection); an external rate-limiter dependency; per-route
  role decorators (a verb gate is enough for a single shared token).

### 2026-06-10 — Browser auth = opaque HttpOnly cookie session, gate pages, stop templating the token (PR #107) [accepted]
- **Choice**: when `ui_auth_token` is set, add a `/login` page that mints an opaque
  in-process session id stored as an `HttpOnly`/`SameSite=Lax` cookie (`Secure` derived
  from scheme/`X-Forwarded-Proto`); gate the page routes (unauth → 303 `/login`); the
  API guard accepts the cookie as well as `Bearer`/`?token=`; and the raw token is no
  longer injected into any served page. Open-redirect guard on `next`; the urlencoded
  login form is parsed by hand to avoid a `python-multipart` dependency.
- **Why**: exposing the UI remotely made the server-templated token a leak (anyone who
  loaded a page got it). Cookies are the browser-native auth, work with header-less SSE
  (`EventSource` sends same-origin cookies), and `SameSite=Lax` protects the mutating
  POST/DELETE routes from CSRF. Linchpin of Epic B.
- **Rejected alternatives**: signed/stateless JWT (overkill for one shared secret);
  keeping the templated token (the leak); HTTP Basic (no logout, worse UX);
  `SameSite=None` (re-opens CSRF); adding `python-multipart` just to read one field.

### 2026-06-10 — Remote UI exposure = TLS reverse proxy or private overlay; app stays on loopback (PR #106) [accepted]
- **Choice**: document remote UI access (`how-to/expose-remote`) as either (a) a TLS
  reverse proxy (Caddy blessed; nginx / Cloudflare Tunnel alternatives) with dccd kept
  on `ui_host: 127.0.0.1`, or (b) a private Tailscale/WireGuard overlay where binding
  `0.0.0.0` is acceptable because the tailnet is already encrypted+authenticated. The
  `ui_auth_token` is defence-in-depth, not transport security. First leaf of Epic B.
- **Why**: the API must never travel plaintext off-box; TLS termination belongs in a
  proxy (auto Let's Encrypt) or is provided by the overlay, not baked into the app.
- **Rejected alternatives**: binding `0.0.0.0` on a public IP with only the token (no
  encryption); implementing TLS inside the app (proxies do it better, auto-renew);
  documenting only one path (Tailscale-only would exclude public VPS users).

### 2026-06-10 — Blessed deploy path = systemd (venv), Docker as alternative (PR #102) [accepted]
- **Choice**: `how-to/deploy` documents **systemd + a venv** as the recommended path
  for a long-lived server, with **Docker** as the containerised alternative (not the
  default). Closes Epic A.
- **Why**: a home/VPS box that runs dccd 24/7 wants the lightest persistent setup —
  no container runtime, native journald logs, clean `Restart=`/reboot semantics, a
  stable data dir via `StateDirectory`. Docker suits ephemeral/orchestrated hosts but
  adds an engine + volume indirection for the common single-box case. Both paths were
  executed and verified on a real Ubuntu 24.04 server (build/install, `/health`,
  reboot survival, alerts, healthcheck).
- **Rejected alternatives**: Docker-first (heavier for the common case); documenting
  both as equal (no guidance — readers want one blessed path); a compose stack
  (over-engineered for a single service).

### 2026-06-10 — Wire HealthMonitor into the daemon + key alerts by job (PR #100) [accepted]
- **Choice**: instantiate `HealthMonitor` in both daemon entry points — `cmd_start`
  (on the scheduler's bus) and the API lifespan (standalone `dccd ui` only, to avoid
  double-wiring). Key the consecutive-failure counter on the **job (spec id)**, not
  the unique per-run `run_id`. Healthcheck via Docker `HEALTHCHECK`/`systemd`; logs
  via journald (no custom file logger); resource limits shipped commented.
- **Why**: `HealthMonitor` was dead code — never instantiated — so alerts never
  fired (same class as `RemoteStorage` pre-Epic-C). And its per-`run_id` keying could
  never accumulate across backfill runs (each run id is `{spec}@{ts}`), so only
  streams (stable `@stream` id) could ever alert. Verified live: a failing job past
  the threshold delivered a real webhook POST to a sink; the container is `healthy`.
- **Rejected alternatives**: wire it only in `cmd_start` (then `dccd ui` never
  alerts); a custom file logger + logrotate (journald/docker already own rotation —
  a second mechanism to maintain); `WatchdogSec` (needs `sd_notify` wiring dccd
  doesn't have — would kill a healthy daemon); forced resource limits (can OOM-kill a
  busy daemon — shipped commented with guidance instead).

### 2026-06-10 — Restart safety is reconstruction-from-config, verified by reboot (PR #99) [accepted]
- **Choice**: keep restart safety as *stateless reconstruction* — the daemon holds
  no cross-process state; on boot it rebuilds everything from `config.yml` +
  on-disk stores (`cmd_start` → `scheduler.start(cfg.all_job_specs())`) and resumes
  from the coverage manifest / `store.last_timestamp`. No checkpoint/PID file. A real
  `systemctl reboot` is the acceptance test, plus `test_restart.py` as the guard.
- **Why**: config + the SQLite WAL stores (`runs.db`, `coverage.db`) are already the
  durable truth; a separate restart-state file would be a second source to keep in
  sync. The real reboot confirmed: service auto-active, stream reconnected (trades
  2000→3000 contiguous), interval re-armed, `runs` 6→12 (append), coverage intact.
- **Rejected alternatives**: a checkpoint/resume file (duplicates config + stores,
  drifts); relying on systemd to re-run a one-shot (loses the live stream). No code
  change was needed — this records *why* and adds the regression guard.

### 2026-06-10 — systemd deploy: venv `ExecStart` + `StateDirectory` (PR #98) [accepted]
- **Choice**: `deploy/dccd.service` runs dccd from a venv at `/opt/dccd/venv` and
  uses `StateDirectory=dccd` (systemd creates/owns `/var/lib/dccd` for `User=dccd`),
  rather than a system-wide `pip install` at `/usr/local/bin/dccd` + a manual
  `useradd --create-home`.
- **Why**: Ubuntu 24.04 is PEP 668 (externally-managed) — a system pip install needs
  `--break-system-packages`; a venv is clean and isolated. The old hard-coded
  `ExecStart=/usr/local/bin/dccd` failed `systemd-analyze verify` on a real host.
  `StateDirectory` removes the manual mkdir/chown and guarantees correct ownership
  under `ProtectSystem=strict`. Verified live (install, auto-restart, hardened write).
- **Rejected alternatives**: system-wide pip (`--break-system-packages`, pollutes the
  system env); `DynamicUser=yes` (loses a stable uid for the data dir across
  restarts); manual `useradd --create-home` + mkdir (more steps, easy to get perms
  wrong). Also fixed: `.[daemon,ui]` referenced a non-existent `ui` extra → `.[daemon]`.

### 2026-06-09 — Old-CPU support via a `POLARS_VARIANT` build arg + digest-pinned base (PR #97) [accepted]
- **Choice**: the `Dockerfile` pins the base image to a `python:3.12-slim` digest
  and exposes `ARG POLARS_VARIANT=polars`. Modern hosts build unchanged; hosts
  whose CPU lacks AVX2 build with `--build-arg POLARS_VARIANT=polars-lts-cpu`,
  which uninstalls `polars` and installs the LTS-CPU wheel **unpinned** (it lags the
  latest `polars`, so pinning to polars's version fails to resolve).
- **Why**: discovered on the real Epic A test box — an Intel i3-2367M (Sandy Bridge,
  no AVX2/FMA/BMI). The default `polars` wheel crashes the daemon at import with
  SIGILL (exit 132). dccd depends on polars, so the whole app is unusable on such
  CPUs (common for recycled home servers). Real-host verification caught this; unit
  tests never would.
- **Rejected alternatives**: make `polars-lts-cpu` the default everywhere (penalises
  the common modern-CPU case with no AVX2 fast paths); doc-only (the image still
  breaks out-of-the-box on those hosts); pin the variant to polars's exact version
  (unresolvable — lts-cpu trails). The same variable applies to the systemd venv
  install (leaf 02) and is documented in the deploy how-to (leaf 06).

### 2026-06-09 — Hierarchical file-based plan trees + complexity-derived agent execution (PR #94) [accepted]
- **Choice**: plans become durable, hierarchical **files in the repo**
  (`doc/dev/plans/<epic>/`: a global `00-plan.md` + precise leaf specs, adaptive
  depth). Each leaf declares a `complexity` that derives the execution model
  (`low→haiku`/`medium→sonnet`/`high→opus`). The tree lands on `develop` via a
  **"plan PR" first**, then `/execute-leaf` spawns an agent per leaf that must
  **verify on real data**; `/finish-task` archives the leaf and ticks the global,
  the last leaf triggers `/release`. Gated on a `plans_dir` descriptor key (absent
  ⇒ the legacy plan-mode loop — backward compatible).
- **Why**: `plan mode` plans live in `~/.claude/plans/` (lost on `/compact`), and
  the old loop never materialised *whether we were planning one slice or the whole
  set*. Files in the repo are durable, reviewable, and visible to every leaf
  branch; an explicit global+leaf hierarchy fixes the granularity ambiguity; the
  precise leaf level is what makes safe agent handoff possible.
- **Rejected alternatives**: keep ephemeral plan-mode only (the status quo — fails
  durability); one flat plan file per epic (doesn't separate the map from the
  executable detail, and can't express per-leaf model/deps). Note: `~/.claude`
  isn't a git repo, so the skill bodies themselves are applied directly, not via
  this PR — only the repo-tracked parts (descriptor, `doc/dev/plans/`, `CLAUDE.md`)
  ship here.

### 2026-06-09 — Read-through restore in operations.read, whole-dir copy (PR #90) [accepted]
- **Choice**: when `operations.read` finds no local Parquet for a dataset and a
  remote is configured, it `rclone copy`s the dataset's **whole directory** back
  (via `RemoteStorage.restore`, copy not sync — never deletes) before loading.
  The remote is resolved from config (`build_remote`) and threaded into
  `Client.read` and `POST /api/read` (off-thread there, since restore shells out).
- **Why**: the free-space purge (#89) makes a dataset's local files disappear;
  backfill resume already survives via the coverage manifest (#88), but *reads*
  would silently return empty. Pulling the dataset back on a read-miss makes the
  purge transparent. Whole-dir copy keeps it simple and matches the coarse
  annual/daily file layout — a time-sliced restore would add rclone-filter
  complexity for little gain.
- **Rejected alternatives**: restore only the period files overlapping the read
  window (more rclone plumbing, marginal benefit at this file granularity); teach
  `ParquetStore` about remotes (breaks the storage/remote separation — the
  application layer already owns this orchestration, as with sync/purge).

### 2026-06-09 — Free-space purge runs right after a successful sync (PR #89) [accepted]
- **Choice**: a `storage.purge.purge_to_free_space` drops the **oldest** Parquet
  files (by mtime, `.dccd/` excluded) until free space is back above
  `storage.min_free_gb`. The Scheduler calls it **only after a successful sync
  cycle**, off-thread. Free accounting is simulated (start probe + summed file
  sizes) so it's deterministic and unit-testable via a `free_fn` injection.
- **Why**: dropping local files is only safe once they're mirrored off-box;
  tying the purge to a just-completed sync gives that guarantee without tracking
  per-file sync state. Oldest-first keeps recent data local (most likely to be
  read) and offloads cold data. The coverage manifest (#88) preserves the resume
  cursor, so a purged dataset still resumes correctly on the next backfill.
- **Rejected alternatives**: a fixed local-size cap or age-based retention (the
  user asked specifically for "when I run low on disk" → a free-space floor); a
  standalone purge daemon/timer (the sync loop is already the natural,
  safety-gated trigger); deleting during the sync itself (must be strictly after
  the mirror confirms).

### 2026-06-09 — Coverage manifest in SQLite so local data can be dropped safely (PR #88) [accepted]
- **Choice**: a `CoverageStore` (SQLite at `.dccd/coverage.db`) records each
  dataset's `[min_ts, max_ts]` + row count on every successful backfill;
  `backfill(start="last")` consults it (`get_max_ts`) when `store.last_timestamp`
  returns `None`, i.e. the local Parquet is gone. Wired through `service_factory`
  and threaded into backfill by every caller (Client, CLI, API, Scheduler).
- **Why**: backfill resume reads the cursor from *local Parquet only*, so dropping
  files to free disk (the point of Epic C) would make the next run re-download
  from the bounded default lookback. A small manifest that lives outside the data
  files (and is never purged) is the cheapest durable cursor. Chosen over a
  remote-aware inventory (rclone listing on every run — network + remote-availability
  coupling). The envelope only ever *widens* (min of mins, max of maxes) so a
  narrow re-backfill can't shrink recorded coverage.
- **Rejected alternatives**: query the remote for what exists (slow, couples
  resume to remote uptime); store the cursor inside the Parquet footer (lost with
  the file — the exact failure we're avoiding); reuse `RunsStore` (runs are an
  event log, coverage is current-state per dataset — different shape and lifecycle).

### 2026-06-09 — Surface remote sync in the Storage UI; share one sync-cycle primitive (PR #87) [accepted]
- **Choice**: extract the single sync cycle (create run → `sync_all` → finish +
  events) into `operations.sync_remote`, reused by both the scheduler loop and a
  new manual `POST /api/storage/sync`. The Storage page reads `GET
  /api/storage/sync` (last/next/volume/remotes) and gets a "Sync now" button. The
  manual endpoint resolves the remote from **config** (`app.state.remote =
  build_remote(cfg)` in the lifespan), not from the scheduler.
- **Why**: in `dccd ui` mode the standalone scheduler has no remote wired in, so
  reading it from the scheduler would make "Sync now" silently dead there;
  resolving from config works in both `dccd ui` and `dccd start`. Sharing
  `sync_remote` keeps run-recording in one place so the manual and scheduled
  paths can't drift. The page persists status (a plain GET) via the existing
  `sync` runs; no SSE — kept deliberately light per the user's ask.
- **Rejected alternatives**: duplicate the run-recording in the endpoint (drift
  risk); drive the sync from `scheduler._remote` (dead in `dccd ui`); push live
  status over SSE on the Storage page (heavier than asked — a 10s poll suffices).

### 2026-06-09 — Daemon drives rclone sync; own the loop in the Scheduler (PR #86) [accepted]
- **Choice**: schedule the existing `RemoteStorage.sync_all` from `dccd start` by
  giving `Scheduler` an optional `remote`/`sync_interval` and a `_sync_loop`
  (mirroring the existing interval-loop pattern), and record each cycle as a
  `sync` run in `RunsStore` (zero schema change) while emitting live `remote-sync`
  EventBus status. Wiring goes through `service_factory.build_remote`.
- **Why**: `RemoteStorage` was fully implemented but never instantiated outside
  tests, so a server `dccd start` mirrored nothing off-box. Owning the loop in the
  Scheduler makes it unit-testable without rclone and gives clean teardown via the
  existing `stop()`. Persisting a run row makes the sync observable after the fact
  (the Storage page is a plain GET) — the keystone the upcoming UI (PR2) reads.
  This is PR 1 of a 4-PR Epic C (tiered storage: sync → UI → coverage manifest →
  free-space purge).
- **Rejected alternatives**: firing rclone directly from `cmd_start` (untestable,
  no reconcile/stop, no status surface); a new always-on service object (more
  wiring than a scheduler loop for no gain); EventBus-only with no persistence
  (status vanishes on UI refresh, so the Storage page couldn't show "last sync").

### 2026-06-07 — v3 docs sweep: drop the v2 migration story, consolidate examples (PR #82) [accepted]
- **Choice**: removed the README "Migrating from v2" section (and every `dccd
  migrate` mention) outright rather than keeping a slim note; consolidated
  `examples/` to v3 by deleting the redundant `historical_downloader.ipynb` and
  rewriting the rest onto `Client` / `dccd.application` / the v3 `jobs:` config.
- **Why**: the `dccd migrate` tool was already deleted in v3 (see the
  Changed/Removed CHANGELOG entry), so the section documented a command that no
  longer exists — a slim note would still imply an upgrade path that isn't there.
  The two `historical_downloader.{py,ipynb}` were byte-for-byte the same v2 API;
  keeping both doubles the maintenance surface for no added coverage. The v3
  config now uses one `jobs:` list, so `settings.data_path` (what `build_store`
  reads) is the canonical store root, not the legacy `storage.local_path`.
- **Rejected alternatives**: keep a "breaking changes" note without commands
  (still implies a migration path; the CHANGELOG already records the v2→v3
  rupture); update both example downloaders in place (redundant — the notebook
  added nothing the script didn't).

### 2026-06-07 — Tooled dev loop + single-source roadmap (PR #79) [accepted]
- **Choice**: `doc/dev/07-roadmap.md` is the single source of open work (root
  `TODO.md` dropped); the dev loop is tooled by `/pick-task` → `/finish-task` /
  `/abandon-task`, with this journal capturing the *why* of each PR and
  `/groom-docs` keeping `doc/dev/` lean.
- **Why**: the roadmap was duplicated (gitignored `TODO.md` vs tracked roadmap)
  and decisions/negative knowledge weren't captured — the exact drift the v3
  retrospective flagged ("doc périmée"). One tracked source + capture-at-PR-time
  kills it.
- **Rejected alternatives**: a git hook that writes decisions from the diff
  (can't reconstruct the *why* post-hoc — capture must happen while the context
  is live, i.e. in the skills); keeping the `TODO.md` / roadmap split (leaves a
  clean checkout blind to the backlog).
