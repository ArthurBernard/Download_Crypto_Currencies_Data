---
plan: epic-a-remote-server/04-resource-ops
kind: leaf
status: planned
complexity: medium
depends: [01, 02]
parallel: false
branch: feat/resource-ops
pr: ""
---

# Resource/ops: healthcheck, log rotation, limits, alerting

## Goal
Wire the operational basics for an unattended deploy: `/health` consumed by the
orchestrator, log rotation, basic resource limits, and failure alerting via the
existing `HealthMonitor` webhook.

## Files to change
- `Dockerfile` — add a `HEALTHCHECK` hitting `GET /health` (the endpoint exists at
  `dccd/interfaces/api/app.py:688`).
- `deploy/dccd.service` — add `WatchdogSec=` + a systemd-side health note, and
  basic limits (`MemoryMax=`, `CPUQuota=`, `TasksMax=`) commented with sane
  defaults; rely on journald for logs (document rotation via
  `journald`/`logrotate` rather than inventing a file logger).
- `dccd/application/monitor.py` — confirm `HealthMonitor` subscribes to the
  EventBus and fires the webhook on repeated failures; wire it into the daemon boot
  (`cmd_start` / API lifespan) if it isn't already instantiated there.
- (If log output isn't structured enough for journald) a minimal
  `logging.basicConfig` in the daemon entry — only if needed; prefer not adding a
  file handler (journald/docker logging owns rotation).

## Steps
1. Add the Docker `HEALTHCHECK` and confirm `docker inspect` shows healthy after
   boot.
2. Confirm `HealthMonitor` is actually constructed and subscribed in the running
   daemon (grep the boot path); if not, wire it from `settings` (webhook url).
3. Add commented resource limits to the systemd unit; document the journald log
   story (`journalctl -u dccd`, rotation via journald config) — no custom file log.
4. Trigger a simulated repeated failure (a job that errors) and confirm the webhook
   fires once past the threshold.

## Tests
- `dccd/tests/v3/test_application.py`: a `HealthMonitor` test (if missing) — feed
  it N failing `StatusEvent`s and assert the webhook callback fires once past the
  threshold (mock the HTTP post). Don't hit a real webhook in tests.

## Verification on real data
- Run the daemon, post a real (or local stub) webhook URL, drive a failing job,
  and observe the alert. Confirm `/health` flips/stays per the orchestrator
  (`docker inspect` health / `systemctl` watchdog).
- If Docker/systemd unavailable here, verify the `HealthMonitor` path live in a
  plain `dccd start` and flag the orchestrator-side wiring for a real host.

## Closeout
- CHANGELOG (`Added`): "Ops hardening for unattended deploy: Docker `HEALTHCHECK`
  on `/health`, systemd watchdog + resource limits, `HealthMonitor` webhook wired
  into the daemon, journald log rotation documented (#NN)".
- ADR: record the choice "journald/docker logging owns rotation (no custom file
  logger)" and the healthcheck/limits defaults.
- Status/roadmap: deferred to last leaf (06).
