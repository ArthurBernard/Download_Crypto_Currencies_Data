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

## Epic E — Audit 2026-06-10 fixes (correctness, perf, prod hygiene)

Source: full-repo audit of 2026-06-10 (session doc `AUDIT-2026-06-10.md`, not
committed). Ordered by priority; B6/B3 are confirmed in production
(~350 zombie `running` runs in arthurserver's runs.db).

- [ ] **B6 — stream `NoCapability` leaks zombie runs** — in
  `operations.stream()` the capability check sits before `create_run`'s
  `try` → the run row is inserted but never finished; and
  `_StreamWorker._run_forever` retries a *permanent* error forever (60 s
  loop). Move the check before `create_run` (or into the `try`) **and** make
  the supervisor abandon on permanent errors.
- [ ] **B3 — purge orphan `running` runs at daemon startup** — `RunsStore`
  never marks runs left `running` after a crash as `stale`/`failed`;
  `active_runs()` returns them forever.
- [ ] **B2 — time-based flush for trades/OHLC streams** — streams only flush
  every 1000 records; a quiet pair keeps hours of data in RAM (lost on
  crash, invisible on disk). Add a time-based flush (e.g. 60 s).
- [ ] **B5/P1 — RateLimiter fate + one HTTP pool per operation** —
  `RateLimiter` is wired nowhere (doc says it is); each paginated page
  currently opens/closes a fresh `httpx.AsyncClient` (1 TLS handshake per
  page). Decide: wire the limiter into adapters or delete it + fix
  CLAUDE.md; and hold the shared client open for the whole operation.
- [ ] **Prod config — off-box backup + alerts + systemd limits** — no rclone
  remote configured (prod data backed up nowhere), no alert webhook,
  no `MemoryMax`/`TimeoutStopSec` in the unit (62 s stop vs 90 s default).
  Ops + how-to doc, on arthurserver.
- [ ] **UX — gap % counts empty minutes as missing** — illiquid pairs show
  misleading "85 % missing" (exchanges emit no empty candles). Distinguish
  true holes from trade-less minutes, or label it in the Data UI.
- [ ] **Tests — CLI 0 %, adapter payload fixtures, stop/cancel path** — Typer
  `CliRunner` suite; recorded REST/WS payload fixtures for adapters
  (38–63 % coverage today, network-only); cover `_StreamWorker.stop()` and
  the no-capability stream case (B6 regression test).
- [ ] **Minor batch** — dynamic OpenAPI version (`3.0.0` hardcoded), CI job
  for the Sphinx 0-warning rule, streams' `rows_written` always 0, restart
  delay never resets after a healthy period, purge 21 merged local branches.

P2 (append+compaction writes) and P3 (filename-based pruning in `load()`)
stay parked below as perf ideas until load demands them.

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
