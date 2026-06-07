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

## Pending

- **Release**: v3 is `3.0.0` in `pyproject` but lives on `develop` and is **not
  tagged**. Last published tag is `v2.4.0`. Releasing = `develop → master` +
  `v3.0.0` tag (use the `release-gate` skill).

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
- **Remote data sync** (`storage/remote.py`, rclone) exists; confirm it's actually
  scheduled by `dccd start` before relying on it unattended (see roadmap).

## Tooling & infra present in the repo

- **Deploy**: `Dockerfile` (python:3.12-slim, runs the daemon; bind `0.0.0.0`,
  mount `/data`) and `deploy/dccd.service` (systemd unit for `dccd start`).
- **Scripts**: `scripts/repair_kraken_okx.py` (one-off data repair).
- **Examples**: `examples/` (v3 config sample, `Client` downloader script, and a
  `dccd.application` daemon example).
- **Project skills** (`.claude/skills/`): `data-e2e` (real-data verification),
  `release-gate` (pre-release checks), `ui-audit` (browser audit). Plus the
  user-level `/finish-task` flow used to open PRs.

## Deferred (post-3.0, "M3")

MCP server interface, Kraken OHLC derivation from trades, derivative markets
(perp/futures, funding/OI), and private/authenticated endpoints. Not started;
expected after the 3.0 release.

## What's next

See [`07-roadmap.md`](07-roadmap.md): run the app on a remote server, reach the UI
remotely from a PC/mobile, and sync data to remote storage.
