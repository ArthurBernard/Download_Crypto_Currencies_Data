---
plan: perf-robustness/04-scheduler-monitor-hygiene
kind: leaf
status: done
complexity: medium
depends: []
parallel: true
branch: fix/scheduler-monitor-hygiene
pr: "#XX"
---

# Scheduler backoff + startup jitter; HealthMonitor alert cooldown

## Goal

A permanently failing interval job (production case: a `FOO/USDT` job) re-runs
at full cadence forever, and `HealthMonitor` fires a webhook on **every**
failure past the threshold — the journal showed an alert every ~20 s for hours.
At daemon start all interval jobs also fire simultaneously (thundering herd on
exchanges and disk). Add failure backoff, startup jitter, and an alert
cooldown.

## Files to change

- `dccd/application/scheduler.py` —
  - `_run_once(spec)` returns `bool` (True = backfill returned without an
    `error` key / no exception; the `backfill()` result dict already carries
    `error` on failure — use it, don't re-parse logs).
  - `_interval_loop(spec)`:
    - **startup jitter**: before the first run, `await asyncio.sleep(
      random.uniform(0, min(every, 60)))` so 50 jobs don't fire in the same
      second at boot;
    - **failure backoff**: on consecutive failures sleep
      `min(every * 2**k, max(every, 6 * 3600))` instead of `every`
      (k = consecutive failures, capped exponent to avoid overflow); reset on
      success. Log the chosen delay at WARNING on each failed iteration.
- `dccd/application/monitor.py` —
  - alert when the consecutive count **crosses** `max_consecutive_errors`
    (`count == self._max_errors`), then re-alert at most once per cooldown
    window (`_ALERT_COOLDOWN_S = 3600`, monotonic clock per key) while the
    failure persists; reset both count and cooldown timestamp on success.
  - keep one WARNING log per *suppressed* webhook failure? No — log the
    webhook-send failure itself only once per cooldown window too (the
    journal had `Webhook alert failed: Connection refused` every 20 s).

## Steps

1. Scheduler changes (import `random` at module top).
2. Monitor cooldown.
3. `pytest`, `ruff check dccd/`, `mypy dccd/`.

## Tests

- `dccd/tests/v3/test_application.py` (or a new `test_scheduler_hygiene.py`),
  using a fake registry/adapter that fails deterministically and
  `asyncio` time control (wrap sleeps via a monkeypatched `asyncio.sleep`
  recording requested delays — the existing test style for the scheduler):
  - failing interval job: recorded sleep delays grow `every, 2*every, 4*every…`
    capped; a success resets to `every`.
  - first sleep before any run is within `[0, min(every, 60)]` (jitter).
  - monitor: with `max_consecutive_errors=3`, 10 consecutive failures fire
    exactly 1 alert (cooldown not elapsed); advance the monotonic clock past
    the cooldown → exactly 1 more; a success in between resets the count so
    the next 3 failures alert again.

## Verification on real data

- Local `dccd start` with one deliberately invalid backfill job
  (`FOO/USDT` on binance, `every: 30`) and a valid one: journal/log output
  over ~5 minutes shows growing gaps between FOO attempts, exactly one alert
  line, and the valid job unaffected. (No isolated-store data assertion needed
  beyond "valid job still writes" — check its parquet row count grows.)

## Closeout

- CHANGELOG (`Fixed`): "Permanently failing scheduled jobs no longer hammer the
  exchange at full cadence (exponential backoff, reset on success), interval
  jobs start with jitter instead of all at once, and HealthMonitor alerts once
  at the failure threshold then at most hourly — instead of on every failure.
  (#NN)"
- ADR: none — mechanical hygiene; thresholds (60 s jitter cap, 6 h backoff cap,
  1 h alert cooldown) noted as constants, not config, until someone needs them
  configurable.
- Status/roadmap: tick leaf 04 in `00-plan.md`.
