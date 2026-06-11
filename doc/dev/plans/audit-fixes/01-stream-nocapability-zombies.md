---
plan: audit-fixes/01-stream-nocapability-zombies
kind: leaf
status: done
complexity: medium
depends: []
parallel: false
branch: fix/stream-nocapability-zombies
pr: ""
---

# B6 — stream `NoCapability` leaks zombie runs; supervisor retries forever

## Goal

A stream job whose adapter lacks the live capability must fail *cleanly*: no
orphan `running` row in runs.db, and the supervising worker must stop instead
of retrying a permanent error every 60 s. Confirmed in prod (~350 zombie rows
from `stream:bitfinex:*:orderbook`).

## Files to change

- `dccd/application/operations.py` — in `stream()`, the
  `adapter.capability_for(target.data_type, "ws", "live")` check (currently
  ~line 421) runs *after* `runs_store.create_run(...)` (~line 384) and
  *outside* the `try` → the row is inserted, `finish_run` never called.
  Move the adapter lookup + capability check **before** `create_run`. The
  three `isinstance` re-checks inside the `try` stay (they're already covered
  by the failure path).
- `dccd/application/scheduler.py` — `_StreamWorker._run_forever()`:
  - catch `NoCapability` separately: log an error ("permanent, not
    retrying"), emit `events.status("failed")` + an error log on the run
    events, and `return` (worker stops; `is_running` becomes False).
  - reset the backoff after a healthy run: record `time.monotonic()` before
    `await stream(...)`; in the generic `except Exception` branch, if the
    stream ran longer than e.g. 300 s before failing, reset `delay = 5.0`
    before applying it (fixes "every restart waits 60 s after a few blips
    over weeks").

## Steps

1. In `operations.stream()`, hoist `adapter = registry.get(...)` and the
   `capability_for` check above the `runs_store.create_run` block, so
   `NoCapability` propagates before any run row exists.
2. In `_StreamWorker._run_forever`, import `NoCapability` from
   `dccd.domain.errors`; add the dedicated except branch that abandons.
3. Implement the backoff reset (healthy-duration threshold 300 s).
4. Run `pytest`, `ruff check dccd/`, `mypy dccd/`.

## Tests

- `dccd/tests/v3/test_application.py` (or a new `test_scheduler_worker.py`):
  - `stream()` with a registry whose adapter declares no live capability and
    a real temp `RunsStore` → raises `NoCapability` AND `active_runs()` is
    empty (regression for B6).
  - `_StreamWorker` over a stub `stream` that raises `NoCapability` → after
    `start()` + a short sleep, `is_running` is False and no unbounded retry
    happened (count calls).
  - Backoff reset: stub stream fails fast twice (delay grows), then runs
    600 s (mock monotonic) and fails → next delay is 5 s again.

## Verification on real data

- Isolated store + config (`/tmp/dccd-b6`): add a stream job for a
  combination with no WS capability (e.g. `bitfinex` orderbook, or bybit spot
  trades history analogue for live), `dccd ui` it, wait 3 min: assert runs.db
  contains **zero** `running` rows for that spec and the log shows a single
  permanent-failure line, not one per minute.
- Sanity: a valid stream (binance BTC/USDT trades) still starts, collects,
  and stops cleanly.

## Closeout

- CHANGELOG (`Fixed`): "Stream jobs without a live capability no longer leak
  zombie `running` runs, and their supervisor stops retrying a permanent
  error; stream restart backoff resets after a healthy period (#NN)"
- ADR: permanent vs transient stream errors — supervisor semantics.
- Status/roadmap: tick leaf in 00-plan; roadmap line stays until last leaf.
