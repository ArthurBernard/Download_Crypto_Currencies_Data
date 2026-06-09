# 6 — Current status

A snapshot of what's done, what's pending, and what's deliberately deferred — so
an agent doesn't re-investigate settled ground or assume missing things are bugs.

## Done & working

- **Hexagonal core (P0–P7)**: domain/transport/sources/storage/application/
  interfaces all in place; 7 exchanges; `Client` + CLI + HTTP API + Web UI on one
  registry (parity enforced by test).
- **Collection correctness**: capability-driven engine; cursor-based trades
  pagination; bounded first backfill; cancellable backfills; honest capabilities.
- **Storage**: ns Parquet with provenance + per-type dedup; defensive legacy
  reads (`canonicalize` on merge). The one-shot v2→v3 migration tool was run on
  the real ~120-file dataset (zero loss, backed up first) and has since been
  **removed** — the data is migrated; new installs start on the v3 schema.
- **Web UI** (PR #76 + polish): Data / Historical / Live / Dashboard / Logs /
  Config / Storage; inline job CRUD; SSE liveness; order-book best bid/ask +
  cadence fixed across all WS adapters.
- **Security**: Bearer auth on `/api/*` when `ui_auth_token` is set; non-wildcard
  CORS via `ui_allow_origins`.
- **Quality gates**: ~191 unit tests + 3 network E2E (opt-in); `ruff` + `mypy`
  clean; Sphinx 0 warnings; CI matrix 3.11–3.13.
- **Released**: `v3.0.0` tagged on `master` (2026-06-07), superseding `v2.4.0` —
  GitHub Release published. `develop` and `master` are level; the next feature
  merge into `develop` reopens the rolling release PR (`/release next-cycle`).

## Pending

_(nothing release-blocking — see the roadmap for the next epics)_

## Known gaps / sharp edges (by design or deferred)

- **Kraken deep OHLC** isn't auto-derived from trades yet. REST gives 720 recent
  bars; `domain/transforms.aggregate_ohlc` exists but the engine doesn't wire
  trades→OHLC derivation. Deep Kraken OHLC currently isn't collectable.
- **Coinbase & Bitfinex live order book** are not implemented (capabilities not
  declared → `NoCapability`). Their other channels work.
- **Coinbase trades history** is slow (cursor walks recent-first); deep history is
  impractical.
- **Order-book history** doesn't exist on any exchange for free — you build it by
  recording the live WS over time (snapshot per `snapshot_interval`).
- **Remote data sync** (`storage/remote.py`, rclone) is now **scheduled by
  `dccd start`** — a periodic loop mirrors the store off-box every
  `storage.sync_interval` (backoff + persisted `sync` runs + `remote-sync`
  EventBus status), and the **Storage page shows last/next sync + volume with a
  "Sync now" button**. A **coverage manifest** (`CoverageStore`, `.dccd/`) now
  records each dataset's extent so `start="last"` resumes from it when local files
  are gone — local data can be dropped without a re-download. A **free-space
  purge** (`storage.min_free_gb`) drops the oldest already-synced files after each
  sync to stay above the floor, and **read-through restore** pulls a purged
  dataset back from the remote on read. **Epic C (tiered storage) is complete** —
  provisioning, restore and integrity are documented in
  `doc/source/how-to/sync-remote.rst`.

## Tooling & infra present in the repo

- **Deploy**: `Dockerfile` (python:3.12-slim, runs the daemon; bind `0.0.0.0`,
  mount `/data`) and `deploy/dccd.service` (systemd unit for `dccd start`).
- **Scripts**: `scripts/repair_kraken_okx.py` (one-off data repair).
- **Examples**: `examples/` (v3 config sample, `Client` downloader script, and a
  `dccd.application` daemon example).
- **Project skills** (`.claude/skills/`): `data-e2e` (real-data verification),
  `release-gate` (pre-release checks), `ui-audit` (browser audit). Plus the
  user-level loop skills used to open PRs.
- **Dev loop = hierarchical plan trees**: `doc/dev/plans/<epic>/` holds durable
  global + leaf plans (committed; finished ones archived to `_archive/plans/`).
  The chain is `/pick-task → /plan` (build tree + plan PR) `→ /execute-leaf`
  (agent per leaf, model from `complexity`, real-data verify) `→ /finish-task`
  (per leaf: tests/ADR/PR/archive/tick) `→ … → /release`. Format reference:
  `doc/dev/plans/README.md`; descriptor key `plans_dir` in `.claude/workflow.json`
  (absent ⇒ legacy plan-mode loop).

## Deferred (post-3.0, "M3")

MCP server interface, Kraken OHLC derivation from trades, derivative markets
(perp/futures, funding/OI), and private/authenticated endpoints. Not started;
expected after the 3.0 release.

## What's next

See [`07-roadmap.md`](07-roadmap.md): run the app on a remote server, reach the UI
remotely from a PC/mobile, and sync data to remote storage.
