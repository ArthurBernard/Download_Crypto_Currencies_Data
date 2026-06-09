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
### YYYY-MM-DD — <short title> (PR #NN)  [accepted|rejected|tombstone]
- **Choice**: …
- **Why**: …
- **Rejected alternatives**: …
```

<!-- new entries below, newest first -->

### 2026-06-09 — Surface remote sync in the Storage UI; share one sync-cycle primitive (PR #XX) [accepted]
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
