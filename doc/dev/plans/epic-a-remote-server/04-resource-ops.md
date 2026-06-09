---
plan: epic-a-remote-server/04-resource-ops
kind: leaf
status: planned
complexity: high
depends: [01, 02]
parallel: false
branch: feat/resource-ops
pr: ""
---

# Resource/ops: wire HealthMonitor, healthcheck, limits, log rotation

## Goal
Make the unattended deploy operable: **actually wire `HealthMonitor`** into the
daemon (it is currently dead code), expose `/health` to the orchestrator (Docker
`HEALTHCHECK` / systemd watchdog), set basic resource limits, document the journald
log story, and fire a **real webhook alert** past the failure threshold.

## Context / ground truth (verified)
- **`HealthMonitor` is never instantiated in the daemon.** `grep -rn "HealthMonitor("
  dccd/` (excluding tests) returns nothing — `cmd_start` (`cli/main.py:159-197`) and
  the API lifespan (`api/app.py:159-195`) build the scheduler but no monitor. So
  alerts never fire in production. This is the load-bearing fix of this leaf (same
  class as `RemoteStorage` being dead before Epic C).
- The config already carries it: `AlertConfig(webhook_url, max_consecutive_errors=3)`
  (`config.py:82-85`); `HealthMonitor(runs_store, event_bus, webhook_url,
  max_consecutive_errors)` subscribes to the bus and POSTs `{"text": …}` on N
  consecutive `StatusEvent(state="failed")` (`application/monitor.py`).
- `/health` exists (`api/app.py` `@app.get("/health")`).

## Files to change
- `dccd/interfaces/cli/main.py` (`cmd_start`) — instantiate
  `HealthMonitor(runs_store, bus, cfg.alerts.webhook_url,
  cfg.alerts.max_consecutive_errors)` after the bus is built, and **keep a reference**
  (it subscribes in `__init__`, but bind it to a local/closure so it isn't GC'd for
  the daemon's lifetime — mirror the stream-worker GC lesson).
- `dccd/interfaces/api/app.py` (lifespan) — same wiring on `app.state` (so the API
  server path, used by `dccd ui`, also alerts); store as `app.state.monitor`.
- `Dockerfile` — add `HEALTHCHECK --interval=30s --timeout=5s --start-period=20s \
  CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8080/health',timeout=3).status==200 else 1)"`
  (no curl in slim image — use Python, which is present).
- `deploy/dccd.service` — add commented resource limits (`MemoryMax=`, `CPUQuota=`,
  `TasksMax=`) with sane defaults for a 4-core/3.7 G box, and a note on journald log
  rotation (`journalctl -u dccd`, rotation via `journald.conf` — **no custom file
  logger**). Optionally `WatchdogSec=` only if the daemon can `sd_notify` (it can't
  today without extra code → document, don't fake it).
- `dccd/tests/v3/test_application.py` — add a `HealthMonitor` unit test if absent.

## Steps
1. Wire the monitor in both daemon entry points; confirm with
   `grep -n "HealthMonitor(" dccd/interfaces/*/*.py` (now non-empty).
2. Add the Docker `HEALTHCHECK`; rebuild on the box (reuse leaf 01's flow);
   `sudo docker inspect --format '{{.State.Health.Status}}' dccd-verify` → `healthy`.
3. Add commented limits to the unit; reload + restart; `systemctl show dccd \
   -p MemoryMax,CPUQuotaPerSecUSec,TasksMax` reflects them when uncommented.
4. **Real alert test on the box**: start a tiny webhook sink
   (`python3 -m http.server 9999` won't accept POST cleanly — use a 10-line
   `nc -lk 9999` or a trivial Flask/`http.server` POST handler), set
   `alerts.webhook_url: http://127.0.0.1:9999` + `max_consecutive_errors: 2` in the
   config, drive a **failing** job (e.g. a bad symbol / unreachable) so ≥2
   consecutive `StatusEvent(failed)` fire, and confirm the sink receives the POST
   with the alert text.
5. Document the journald rotation knobs in the unit header / leaf-06 hand-off.

## Tests (`dccd/tests/v3/test_application.py`)
- Feed a `HealthMonitor` (real `EventBus`, `runs_store=None`, a fake webhook via
  monkeypatching `urllib.request.urlopen`) N `StatusEvent(state="failed", run_id=r)`
  and assert the webhook is called exactly once at the threshold, and that a
  `succeeded` event resets the counter. No real HTTP.

## Acceptance criteria
- `grep "HealthMonitor(" dccd/interfaces/` is non-empty (wired in CLI **and** API).
- Docker `HEALTHCHECK` reports `healthy`.
- On the box, a real failing job past the threshold delivers a POST to the local
  sink with the alert text.
- New `HealthMonitor` test + full `pytest` green; `ruff`/`mypy` clean.

## Verification on real data
The real alert (step 4) is verified on the box end-to-end (failing job →
EventBus → webhook POST received). Capture the sink's received payload + the
`docker inspect … Health.Status` into the PR.

## Risks / rollback
- Don't add `WatchdogSec` without `sd_notify` wiring (systemd would kill a healthy
  daemon) — document instead, or add `sd_notify` as a small, separately-justified
  change.
- Resource limits too low could OOM-kill a busy daemon — ship them **commented**
  with guidance, not forced.

## Closeout
- CHANGELOG (`Added`/`Fixed`): "Wire `HealthMonitor` into the daemon (CLI + API) so
  webhook alerts actually fire; Docker `HEALTHCHECK` on `/health`; commented systemd
  resource limits + journald log-rotation guidance; real alert verified on a host
  (#NN)".
- ADR: "`HealthMonitor` was dead code — wired into both daemon entry points; logging
  delegated to journald/docker (no custom file logger); watchdog deferred (needs
  `sd_notify`) — why."
- Status/roadmap: deferred to leaf 06; also drop the `06-status.md` caveat once the
  monitor is live if one exists.
