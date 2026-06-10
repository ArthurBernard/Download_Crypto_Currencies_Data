# 7 — Roadmap / next steps

Planned work after the UI rework. The theme: **go from "runs on my machine" to
"runs unattended on a remote server, reachable from anywhere, with data backed up
off-box."** Much of the scaffolding already exists (`Dockerfile`,
`deploy/dccd.service`, `ui_host`/`ui_auth_token`/`ui_allow_origins`,
`storage/remote.py` rclone sync) — these steps are mostly *wiring, hardening, and
UX*, not greenfield.

Status legend: `[ ]` todo · `[~]` partially in place · `[x]` done.

This file is the **single source of truth** for open work — it is read by
`/pick-task` and updated by `/finish-task` / `/abandon-task`. Finished work is
*removed* from here (git log + `CHANGELOG.md` are authoritative for *what*
shipped; `03-decisions.md` for *why*). Keep it short and true.

---

_Epic A — Run the app on a remote server: **done.** Container image (digest-pinned,
`POLARS_VARIANT` for old CPUs) and systemd unit (venv + `StateDirectory`) verified on
a real server; restart/reboot safety, `HealthMonitor` alerts + `/health` healthcheck,
secret injection, and the `how-to/deploy` guide all shipped. See `06-status.md`._

_Epic B — View the UI remotely (PC + mobile): **done.** TLS/overlay exposure guide
(`how-to/expose-remote`), browser `/login` + `HttpOnly` cookie session (token no longer
templated into pages), API hardening (`ui_rate_limit`/`ui_readonly`/`ui_trusted_proxy` +
CORS-never-wildcard test), mobile-responsive pass, and a threat model — all verified
live over Tailscale. See `06-status.md`._

_Epic C — Sync data to a remote space: **done.** Scheduled rclone sync + UI,
coverage manifest, free-space purge, read-through restore, and the
`how-to/sync-remote` guide all shipped. See `06-status.md`._

---

## Suggested sequence

1. ~~**C (sync)**~~ — done.
2. ~~**A (remote run)**~~ — done.
3. ~~**B (remote access)**~~ — done.

The three "runs unattended on a remote server, reachable from anywhere, backed up
off-box" epics are complete. Next axes live under **Deferred — M3** below.

Each epic should ship with a `doc/source/` how-to and, where it touches data,
a pass of the `data-e2e` skill.

---

## Epic D — Performance & robustness (from the 2026-06-10 production audit)

A py-spy profile of the live collector (arthurserver, 3.3.1, 50 jobs) found the
daemon pinned at ~98 % CPU: `kraken.stream_orderbook` rebuilds the full book as
pydantic objects on **every** WS delta while `operations.stream` discards all but
one frame per `snapshot_interval`. The starved event loop made `/api/inventory`
take 100 s for 10 KB (store: 50 files / 32 MB). Full findings: memory
`project-v33-perf-audit` + ADR journal.

Ordered by impact; each line is one small PR:

- [ ] **D1 — Order-book snapshot construction throttled upstream** — adapters keep
  raw dict state and only build `OrderBookSnapshot`/`OrderBookLevel` at capture
  time (`snapshot_interval`), not per delta; Kraken book truncated to the
  subscribed depth (WS v2 contract). Kills the 98 % CPU burn.
- [ ] **D2 — `inventory()` from parquet footer metadata + off-thread + cached** —
  `num_rows` + TS min/max from `pyarrow` footer stats (no column read),
  `asyncio.to_thread` in the API, process-level cache invalidated on write;
  `GET /api/storage/sync` reuses it instead of re-scanning.
- [ ] **D3 — Honest WS subscriptions** — validate `depth` per exchange capability
  (Kraken v2 accepts only {10,25,100,500,1000}; config had 20/50), surface
  subscription error/ACK frames instead of silently filtering them (a "live"
  stream must never sit forever writing nothing).
- [ ] **D4 — Scheduler/monitor hygiene** — exponential backoff for permanently
  failing interval jobs, startup jitter (no thundering herd), HealthMonitor alert
  cooldown (alert on threshold crossing, not every failure).
- [ ] **D5 — HTTP/UI transport efficiency** — `GZipMiddleware`, dashboard fetches
  in parallel, SSE-driven refresh instead of 8–10 s inventory polling,
  `RunsStore` calls off-thread; stream ending on its own reports `failed`/ended,
  not `cancelled`.

## Deferred — M3 (post-3.0)

Larger axes intentionally parked until after the 3.0 release. Not started; do not
treat as bugs (see `06-status.md`).

- [ ] **MCP interface** — `interfaces/mcp/` mapped onto the operation registry
  (same parity contract as API/CLI).
- [ ] **Kraken deep OHLC from trades** — a `DerivedOHLCSource` wiring
  `domain/transforms.aggregate_ohlc` into the resolver (REST only gives 720 recent
  bars; the transform exists but isn't wired).
- [ ] **Derivative markets** — `DataType` for funding / open-interest /
  liquidations, `Symbol.market=perp`.
- [ ] **Auth/secrets for private endpoints** — credential injection into
  `transport/` for authenticated exchange endpoints.
