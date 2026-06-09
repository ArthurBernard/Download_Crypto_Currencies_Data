---
plan: epic-a-remote-server
kind: global
status: planning
roadmap: "Epic A — Run the app on a remote server"
release_on_done: true
---

# Epic A — Run the app on a remote server

## Goal

`dccd start` (scheduler + streams + web UI) runs **24/7 on a VPS/home server**,
surviving reboots and crashes. The scaffolding already exists — `Dockerfile`
(python:3.12-slim, `dccd start --host 0.0.0.0 --port 8080`, `/data` volume,
`XDG_CONFIG_HOME=/etc`) and `deploy/dccd.service` (systemd, `Restart=on-failure`,
`User=dccd`, hardening). Epic A is **verify, harden, and document one blessed
path**, not greenfield: prove both deploy targets actually run, confirm the app
resumes after a restart, wire ops (healthcheck/limits/log rotation/alerting), nail
the secrets story, then write the deploy how-to that records the chosen path.

"Done" = a reader can follow `doc/source/how-to/deploy.rst` to stand up a
self-restarting dccd on a server, with `/health` watched, secrets injected (not
baked), and alerts on repeated failures.

## Decomposition

1. **verify-container** — clean `docker build` + `docker run` (mounted config +
   `/data` volume, UI reachable on `0.0.0.0`); pin/refresh the base image.
2. **verify-systemd** — `deploy/dccd.service`: install path, `Restart=on-failure`,
   `User=dccd`, `ReadWritePaths`, `XDG_CONFIG_HOME`, `/etc/dccd/config.yml`, data
   dir perms; confirm start + auto-restart.
3. **restart-safety** — after a restart, streams resume and the scheduler re-arms;
   `RunsStore` (SQLite WAL) survives; the data volume is durable. Fix if not.
4. **resource-ops** — `/health` wired into the orchestrator (systemd/Docker
   healthcheck), log rotation, basic resource limits, alerting via the existing
   `HealthMonitor` webhook.
5. **secrets-config** — keep `config.yml` out of the image (verify) and document
   env/volume injection of `ui_auth_token` (and `rclone.conf`).
6. **deploy-howto** — `doc/source/how-to/deploy.rst`: one blessed end-to-end path
   (decision: systemd vs Docker recorded as an ADR), referencing the above.

## Leaf checklist

- [ ] 01 verify-container — chore/verify-container — medium
- [ ] 02 verify-systemd — chore/verify-systemd — medium
- [ ] 03 restart-safety — feat/restart-safety — high (depends on 01, 02)
- [ ] 04 resource-ops — feat/resource-ops — medium (depends on 01, 02)
- [ ] 05 secrets-config — docs/secrets-injection — low (depends on 01)
- [ ] 06 deploy-howto — docs/deploy-howto — medium (depends on 01–05)

## Dependencies

- 01 and 02 are independent → **`parallel: true`** (run concurrently).
- 03, 04 depend on 01+02 (need a runnable target); 05 depends on 01.
- 03, 04, 05 are logically independent of each other but run serially (they touch
  overlapping deploy/config docs — keep it simple, no parallel worktrees).
- 06 is the synthesis doc — depends on all of 01–05.

## Done criteria

- Both deploy targets verified to build/run (or the blocker documented if the
  sandbox can't, e.g. no Docker/root — same honesty as the rclone stand-in).
- A real kill→restart cycle shows streams + scheduler resume with no data gap.
- `/health` is consumed by the orchestrator; failures alert via webhook.
- No secret is baked into the image; injection is documented.
- `doc/source/how-to/deploy.rst` builds (0 Sphinx warnings) and records the chosen
  path; the roadmap's Epic A items are all removed by the last leaf's `/finish-task`.
