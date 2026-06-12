---
plan: audit-fixes-2026-06-12/02-boot-orphan-sweep-order
kind: leaf
status: executing
complexity: medium
depends: []
parallel: false
branch: fix/boot-orphan-sweep
pr: ""
---

# Sweep orphaned runs before the scheduler starts, not after

## Goal

Under `dccd start`, `cmd_start` awaits `scheduler.start()` (stream workers create
their `running` rows in runs.db) before `uvicorn` triggers the FastAPI lifespan,
which then calls `mark_stale_running()` and sweeps those legitimate rows to
`stale` ("orphaned by daemon restart"). Move the sweep to `cmd_start` *before*
the scheduler starts, and skip it in the lifespan when a scheduler is injected —
exactly the misuse the `mark_stale_running` docstring warns about.

## Files to change

- `dccd/interfaces/cli/main.py` — `cmd_start`: immediately after
  `runs_store = build_runs_store(...)`, call
  `stale = runs_store.mark_stale_running()` and `typer.echo` a
  "marked N orphaned run(s) stale (daemon restarted)" line when `stale > 0`.
- `dccd/interfaces/api/app.py` — lifespan (~line 197): guard the existing
  `mark_stale_running()` call with `if scheduler is None:` (standalone
  `dccd ui` keeps the sweep); update the comment to say the `dccd start` path
  sweeps in `cmd_start` *before* starting the scheduler, because sweeping here
  would stale-out the stream runs the scheduler just created.
- `dccd/storage/runs_sqlite.py` — `mark_stale_running` docstring Notes: the
  call site is "the daemon boot path, before any new runs are started
  (`cmd_start` for `dccd start`; the FastAPI lifespan for standalone
  `dccd ui`)".
- Tests — see below.

## Steps

1. Add the sweep + echo to `cmd_start` (before `Scheduler(...)`/
   `scheduler.start()`; right after `build_runs_store` is the natural spot).
2. Guard the lifespan call with `if scheduler is None:` and fix the comment.
3. Update the `mark_stale_running` docstring Notes.
4. Add the three tests; run `pytest` and `ruff check dccd/`.

## Tests

- `dccd/tests/v3/test_api.py` —
  `test_lifespan_skips_orphan_sweep_when_scheduler_injected`: point
  `cfg.settings.data_path` at `tmp_path`, pre-create
  `RunsStore(tmp_path/".dccd"/"runs.db")` with one row left in `running`;
  `create_app(config=cfg, scheduler=<Scheduler instance>)`; entering
  `TestClient` must leave the row in state `running`.
- `dccd/tests/v3/test_api.py` —
  `test_lifespan_sweeps_orphans_standalone`: same setup with
  `create_app(config=cfg)` (no scheduler); entering `TestClient` must
  transition the row to `stale` (add only if no existing test already covers
  the standalone sweep).
- `dccd/tests/v3/test_cli.py` — `test_start_sweeps_orphans_before_scheduler`:
  reuse the existing `cmd_start` wiring-test pattern
  (`test_stream_jobs_present_starts_scheduler`, ~line 441): monkeypatch
  `uvicorn.Server.serve` to return immediately, record call order of
  `RunsStore.mark_stale_running` vs `Scheduler.start` (monkeypatched
  recorders appending to one shared list); assert the sweep is called and
  precedes `Scheduler.start`.

## Verification on real data

- Isolated config (`/tmp` store, free port) with one real stream job
  (e.g. `binance BTC/USDT trades`); launch `dccd start`, wait ~15 s, query
  runs.db: the stream's newest run row must be in state `running` with
  `error IS NULL` while the daemon is alive (before the fix it is `stale` /
  "orphaned by daemon restart" within a second of boot). Stop the daemon.
- Restart the daemon once and repeat the check — the *old* row goes `stale`,
  the *new* one stays `running`.

## Closeout

- CHANGELOG `Fixed`: "`dccd start` marked its own just-started stream runs
  `stale` at boot (the orphan sweep ran in the FastAPI lifespan *after* the
  scheduler had started); the sweep now runs in `cmd_start` before the
  scheduler, and the lifespan only sweeps in standalone `dccd ui` (#NN)"
- ADR: none — ordering fix; the constraint is recorded in the
  `mark_stale_running` docstring.
- Status/roadmap: remove the boot-race bullet from
  `## Audit fixes (2026-06-12)` in `doc/dev/07-roadmap.md`; one line in
  `06-status.md` (Dashboard "Active now" shows streams again under
  `dccd start`).
