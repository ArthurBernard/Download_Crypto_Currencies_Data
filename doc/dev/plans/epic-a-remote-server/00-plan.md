---
plan: epic-a-remote-server
kind: global
status: planning
roadmap: "Epic A — Run the app on a remote server"
release_on_done: true
---

# Epic A — Run the app on a remote server

## Goal

`dccd start` (scheduler + streams + web UI) runs **24/7 on a server**, surviving
reboots and crashes. The scaffolding exists — `Dockerfile` (python:3.12-slim,
`dccd start --host 0.0.0.0 --port 8080`, `/data` volume, `XDG_CONFIG_HOME=/etc`)
and `deploy/dccd.service` (systemd, `Restart=on-failure`, `User=dccd`, hardening).
Epic A is **verify, harden, and document one blessed path** — *on a real server*,
not greenfield.

"Done" = a reader follows `doc/source/how-to/deploy.rst` to stand up a
self-restarting dccd on a server, with `/health` watched, secrets injected (not
baked), alerts on repeated failures — and every claim was verified on a live host.

## Test environment (real)

A throwaway Ubuntu box is on the same Tailscale tailnet as the dev machine — see
the `reference-test-server` memory:

- **`ssh dccd-testbox`** → `arthurserver`, `100.91.149.69`, user `arthur`,
  Ubuntu 24.04.1 LTS, x86_64, 4 cores / 3.7 Gi RAM / 32 G disk (~27 G free),
  **passwordless sudo**, key `~/.ssh/dccd_testbox`.
- This converts every "to run on a real host" hedge in the leaves into an
  **actual** verification. Each leaf below gives concrete `ssh dccd-testbox …`
  commands and observable acceptance criteria.

## Execution model (deviation from the default `/execute-leaf`)

These leaves operate a **real, stateful external host**, so they run in the **main
session** (direct `ssh dccd-testbox`), **not** spawned sub-agents — the orchestrator
keeps direct oversight of every privileged/remote command, and the user sees them.
Sub-agents stay appropriate for self-contained, repo-local code work. The real-data
verification discipline still applies: run it on the server, observe, compare.

## Findings already established (ground truth for the leaves)

- **`HealthMonitor` is dead code in the daemon.** `grep -rn "HealthMonitor(" dccd/`
  (excluding tests) returns nothing — neither `cli/main.py:cmd_start` nor the API
  lifespan instantiates it. `AlertConfig` (`webhook_url`, `max_consecutive_errors`,
  `dccd/application/config.py`) exists but is never consumed. → **leaf 04** wires it
  (same class of bug Epic C fixed for `RemoteStorage`).
- **Restart state is reconstructed from config.** `cmd_start` builds everything from
  `cfg` and calls `scheduler.start(cfg.all_job_specs())` (`cli/main.py:159-197`);
  `Scheduler.start` re-creates stream workers + interval loops from the specs
  (`scheduler.py:204-225`). So restart safety is *structurally* present — **leaf 03**
  proves it on a real reboot and checks the resume cursor (Epic C coverage manifest)
  leaves no gap.
- **Ports/paths line up**: `ui_port=8080` (`config.py:44`), Dockerfile `EXPOSE 8080`
  + `CMD --port 8080`; `/health` exists at `api/app.py` (`@app.get("/health")`).
- **Old CPUs without AVX2 need `polars-lts-cpu`** (found in leaf 01 on the test box,
  an Intel Sandy Bridge i3): the default `polars` wheel crashes the daemon at import
  with SIGILL. The `Dockerfile` now takes `--build-arg POLARS_VARIANT=polars-lts-cpu`
  (PR #97, ADR 2026-06-09). **Cross-cutting**: leaf 02's venv install must use the
  lts-cpu variant on this box, and leaf 06 must document the old-CPU caveat.

## Decomposition

1. **verify-container** — real `docker build` + `docker run` on the box (mounted
   config + `/data` volume, UI+`/health` reachable, volume actually written); pin
   the base image to a digest.
2. **verify-systemd** — install `dccd.service` system-wide on the box with a `dccd`
   service user; fix the `ExecStart` path assumption; confirm `Restart=on-failure`
   and the hardening (`ProtectSystem=strict` + `ReadWritePaths`) allow writes.
3. **restart-safety** — real `reboot` of the box: streams reconnect, interval
   backfills re-arm, `RunsStore` (SQLite WAL) survives, no data gap. Fix gaps.
4. **resource-ops** — **wire `HealthMonitor`** into the daemon (the finding above) +
   `/health` healthcheck (Docker `HEALTHCHECK` / systemd watchdog), resource limits,
   journald log rotation, real webhook alert fired.
5. **secrets-config** — prove no secret is baked in the image (`docker history` +
   layer grep) and document env/volume injection of `ui_auth_token` / `rclone.conf`.
6. **deploy-howto** — `doc/source/how-to/deploy.rst`: the blessed path, **followed
   literally on `dccd-testbox`** to validate it; records the systemd-vs-Docker ADR;
   **closes Epic A** (removes the roadmap items, suggests `/release`).

## Leaf checklist

- [x] 01 verify-container — chore/verify-container — medium
- [ ] 02 verify-systemd — chore/verify-systemd — medium
- [ ] 03 restart-safety — feat/restart-safety — high (depends on 02)
- [ ] 04 resource-ops — feat/resource-ops — high (depends on 01, 02)
- [ ] 05 secrets-config — docs/secrets-injection — low (depends on 01)
- [ ] 06 deploy-howto — docs/deploy-howto — medium (depends on 01–05)

## Dependencies

- 01 and 02 are independent → **`parallel: true`** in principle, but both install
  software on the *same* box; run them **serially** here to keep the box state
  legible (01 leaves a Docker engine; 02 a system service). No worktree parallelism.
- 03 depends on 02 (reboot test uses the systemd-managed service — the real
  "survives a reboot" path). 04 depends on 01+02 (healthcheck wiring for both
  targets). 05 depends on 01 (image inspection). 06 depends on 01–05.

## Done criteria

- `docker build`+`run` and a system-wide systemd install both verified on
  `dccd-testbox` (real output captured in each leaf's PR).
- A real `sudo reboot` shows the service back up, streams + intervals resumed, the
  runs DB intact, collection continuing with no gap.
- `HealthMonitor` instantiated in the daemon; a real webhook alert fires past the
  threshold; `/health` consumed by the orchestrator.
- `docker history`/layer grep proves no token/config baked into the image.
- `doc/source/how-to/deploy.rst` builds (0 Sphinx warnings), was executed verbatim
  on the box, and the last leaf removes the Epic A roadmap block → `/release`.
- The throwaway box is left documented; `sudoers.d/99-arthur-nopasswd` noted for
  later revocation.
