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

### 2026-06-10 — Wire HealthMonitor into the daemon + key alerts by job (PR #XX) [accepted]
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
