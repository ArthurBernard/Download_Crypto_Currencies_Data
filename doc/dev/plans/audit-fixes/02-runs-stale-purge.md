---
plan: audit-fixes/02-runs-stale-purge
kind: leaf
status: planned
complexity: medium
depends: []
parallel: false
branch: fix/runs-stale-purge
pr: ""
---

# B3 — purge orphaned `running` runs at daemon startup

## Goal

Runs left in state `running` after a daemon crash/SIGKILL currently pollute
`active_runs()`, `dccd status` and the Dashboard forever (prod: ~350 rows).
At daemon boot, mark them `stale` so the DB reflects reality.

## Files to change

- `dccd/storage/runs_sqlite.py` — new method
  `RunsStore.mark_stale_running(self) -> int`:
  `UPDATE runs SET state='stale', ended_at=<now ns>, error='orphaned by daemon restart' WHERE state='running'`,
  return `rowcount`. Numpydoc docstring.
- `dccd/interfaces/api/app.py` — call it once in the **lifespan startup**
  (the daemon entry point used by both `dccd ui` and `dccd start`), log
  "marked N orphaned runs stale" when N > 0.

**Critical constraint**: the purge must run **only at daemon boot**, never
from one-shot CLI commands (`dccd status`, `dccd backfill`, …) — a CLI call
while a daemon is live would stale-out its legitimate active runs. So: do
NOT put it in `service_factory.build_runs_store()`. Verify `dccd start`
shares the FastAPI lifespan; if it has a separate boot path, call it there
too (and only there).

## Steps

1. Add `mark_stale_running` to `RunsStore`.
2. Call it in the API lifespan before the scheduler starts; check
   `interfaces/cli/main.py` for any daemon boot path that bypasses
   `create_app()` and cover it.
3. Decide nothing about UI: `stale` rows simply stop appearing in
   `active_runs()` (state filter is `WHERE state='running'`) and show as
   `stale` in run history. Check the Logs/Dashboard templates don't choke on
   the new state string (they render state as text/badge).
4. `pytest`, `ruff`, `mypy`.

## Tests

- `dccd/tests/v3/test_storage.py` (or `test_application.py`): create 2
  `running` + 1 `done` run in a temp `RunsStore`; `mark_stale_running()`
  returns 2; `active_runs()` is empty; the rows have state `stale`,
  non-null `ended_at`, and the orphan error message.
- `dccd/tests/v3/test_api.py`: TestClient app boot over a runs DB seeded
  with a `running` row → after startup, `/api/runs` shows it `stale`.

## Verification on real data

- Isolated store: start `dccd ui`, launch a long backfill, `kill -9` the
  process mid-run; restart `dccd ui` → the interrupted run shows `stale` in
  the Logs page and `dccd status` no longer lists it as active.
- On arthurserver (after release lands there): one daemon restart clears the
  ~350 historical zombies — note the count in the leaf report.

## Closeout

- CHANGELOG (`Fixed`): "Runs orphaned in `running` state by a daemon crash
  are marked `stale` at startup instead of polluting active runs forever (#NN)"
- ADR: none — mechanical once the boot-only constraint is stated.
- Status/roadmap: tick leaf in 00-plan.
