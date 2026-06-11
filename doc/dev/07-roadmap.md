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

_Epic D — Performance & robustness (2026-06-10 production audit): **done.**
Order-book snapshots built only at capture time (97.7 % → 2 % CPU), inventory
from parquet footer stats + cache + off-thread, honest WS subscriptions
(declared depths, loud rejections), scheduler backoff/jitter + alert cooldown,
gzip + saner UI polling. PRs #118–#121 + the last leaf; ADR journal has the
rationale; see `06-status.md`._

_Epic E — Audit 2026-06-10 fixes: **done** (PRs #126–#134, 2026-06-11). Zombie
runs, startup purge, time-based stream flush, one HTTP pool per operation,
wired rate limiter, systemd limits, honest coverage metric, CLI + adapter test
suites, OpenAPI version + CI docs gate. ADR journal has the rationale; see
`06-status.md`._

_Prod ops: **complete** (2026-06-11). Off-box backup (hourly rclone sync to
the main PC over Tailscale), ntfy alert webhook (test delivered), systemd
limits, and the server upgraded to v3.5.0 — whose first boot marked 491
orphaned runs `stale`. See `06-status.md`._

P2 (append+compaction writes) and P3 (filename-based pruning in `load()`)
stay parked as perf ideas until load demands them (see the audit doc).

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
