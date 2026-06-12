---
plan: hardening-backlog-2026-06/03-runs-db-retention
kind: leaf
status: planned
complexity: medium
depends: []
parallel: false
branch: feat/runs-db-retention
pr: ""
---

# runs.db boot-time retention

## Goal

The run history is append-only with no purge: ~800 runs/day in production ≈
180 MB/year, unbounded. Add a boot-time retention sweep: delete terminal
non-failed runs (`succeeded`/`stale`/`cancelled`) older than a configurable
number of days (default 90; `failed` rows are kept as the long-term error
journal), then `VACUUM`. Runs on the same boot paths as
`mark_stale_running` — after it, so freshly-staled orphans age normally.

## Files to change

- `dccd/storage/runs_sqlite.py` — new method
  `prune_old_runs(retention_days: int) -> int`: no-op returning 0 when
  `retention_days <= 0`; else
  `DELETE FROM runs WHERE state IN ('succeeded','stale','cancelled') AND
  started_at < now_ns - days*86400*1e9`, then `VACUUM` (only when rows were
  deleted), return the deleted count. numpydoc docstring mirroring
  `mark_stale_running`'s (call sites: daemon boot, after the orphan sweep).
- `dccd/application/config.py` — `SettingsConfig.runs_retention_days: int = 90`
  with a `>= 0` validator (0 disables), documented in the class docstring.
- `dccd/interfaces/cli/main.py` — `cmd_start`: after `mark_stale_running()`,
  call `runs_store.prune_old_runs(cfg.settings.runs_retention_days)` and
  `typer.echo` the count when > 0.
- `dccd/interfaces/api/app.py` — lifespan, inside the existing
  `if scheduler is None:` boot block: same call after `mark_stale_running()`,
  `logger.info` the count when > 0.
- Tests — see below.

## Steps

1. Implement `prune_old_runs` + the settings field (+ validator).
2. Wire both boot paths (`cmd_start`, standalone lifespan) after the orphan
   sweep.
3. Tests; `pytest` + `ruff check dccd/`.

## Tests

- `dccd/tests/v3/test_storage.py` — `test_prune_old_runs`: rows older than
  the cutoff in each terminal state + one old `failed` + one recent
  `succeeded`; prune(90) deletes exactly the old terminal non-failed rows,
  keeps old `failed` and everything recent; returns the count.
  `test_prune_old_runs_disabled`: `prune_old_runs(0)` deletes nothing.
- `dccd/tests/v3/test_api.py` — standalone lifespan prunes: pre-create an
  old `succeeded` row (set `started_at` ~100 days back) in a tmp store,
  enter `TestClient(create_app(config=cfg))`, row gone; with
  `cfg.settings.runs_retention_days = 0`, row survives.
- `dccd/tests/v3/test_cli.py` — extend the boot-order test pattern: under
  `cmd_start`, `prune_old_runs` is called after `mark_stale_running` and
  before `Scheduler.start` (same recorder-list technique as
  `test_start_sweeps_orphans_before_scheduler`).

## Verification on real data

- On a copy of the **production runs.db** (scp from the server to /tmp —
  read-only source, never mutate the live file): run `prune_old_runs(90)`
  via the repo venv, then assert (a) every remaining non-failed terminal row
  is younger than 90 days, (b) `failed` rows are all still there (compare
  counts before/after), (c) the file shrank or stayed equal after VACUUM,
  (d) `list_runs`/`active_runs` still work on the pruned DB.

## Closeout

- CHANGELOG `Added`: "boot-time runs.db retention
  (`settings.runs_retention_days`, default 90, `0` disables): terminal
  non-failed runs older than the window are deleted and the DB VACUUMed at
  daemon start; `failed` runs are kept (#NN)"
- ADR: none — parameters (90 days, keep failed, boot-time) come from the
  roadmap line; note them in the PR body.
- Status/roadmap: remove the runs.db-retention bullet.
