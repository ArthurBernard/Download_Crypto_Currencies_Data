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

## Epic A — Run the app on a remote server

Goal: `dccd start` (scheduler + streams + UI) running 24/7 on a VPS/home server,
surviving reboots and crashes.

- [~] **Container image** — `Dockerfile` exists. Verify a clean `docker build` +
  `docker run` with a mounted config + `/data` volume; pin/refresh base image.
- [~] **systemd unit** — `deploy/dccd.service` exists. Verify install path,
  `Restart=on-failure`, `User=dccd`, `/etc/dccd/config.yml`, data dir perms.
- [ ] **Decide the deployment target** (bare systemd vs Docker vs compose) and
  document one blessed path end-to-end in `doc/source/` (how-to: deploy).
- [ ] **Persistence & restart safety** — confirm streams resume and the scheduler
  re-arms after a restart; `RunsStore` (SQLite WAL) survives; data volume is
  durable.
- [ ] **Resource/ops** — log rotation, healthcheck (`/health`) wired into the
  orchestrator, basic resource limits, alerting via the existing `HealthMonitor`
  webhook.
- [ ] **Secrets/config** — keep `config.yml` out of the image; document env/volume
  injection of `ui_auth_token`.

## Epic B — View the UI remotely (PC + mobile)

Goal: open the dashboard securely from a laptop or phone, not just `localhost`.

- [~] **Bind & auth building blocks** — `ui_host=0.0.0.0`, `ui_auth_token`
  (Bearer), `ui_allow_origins` (CORS) already exist. The default stays
  `127.0.0.1`; remote exposure must be a conscious, documented opt-in.
- [ ] **TLS + reverse proxy** — document a Caddy/nginx (or Cloudflare Tunnel)
  front with HTTPS; the API must never be exposed plaintext off-box. The token in
  `?token=` for SSE only travels over TLS.
- [ ] **Auth UX for browsers** — today the token is injected server-side into the
  template. For true remote access decide the login story: a simple token prompt
  page / cookie session, so a phone can authenticate without editing config.
- [ ] **Mobile responsiveness pass** — audit Data/Historical/Live/Dashboard on a
  narrow viewport (tables → stacked/cards, tap targets, the nav dropdowns). Extend
  `ui_smoke.py` with a mobile viewport run.
- [ ] **Harden for exposure** — rate-limit `/api/*`, confirm no wildcard CORS,
  consider read-only vs control roles, audit that mutating routes require the
  token (they do via the `/api/*` guard — verify under proxy).
- [ ] **Threat model note** — write down the assumptions (LAN vs internet, tunnel
  vs public) in the deploy how-to.

_Epic C — Sync data to a remote space: **done.** Scheduled rclone sync + UI,
coverage manifest, free-space purge, read-through restore, and the
`how-to/sync-remote` guide all shipped. See `06-status.md`._

---

## Suggested sequence

1. ~~**C (sync)**~~ — done.
2. **A (remote run)** — get it running unattended with restart safety.
3. **B (remote access)** — only then expose the UI, behind TLS + auth.

Each epic should ship with a `doc/source/` how-to and, where it touches data,
a pass of the `data-e2e` skill.

---

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
