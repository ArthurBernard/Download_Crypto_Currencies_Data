---
plan: epic-a-remote-server/03-restart-safety
kind: leaf
status: planned
complexity: high
depends: [01, 02]
parallel: false
branch: feat/restart-safety
pr: ""
---

# Persistence & restart safety

## Goal
After a daemon restart (crash or reboot), confirm: configured **streams resume**,
the **scheduler re-arms** its interval backfills, `RunsStore` (SQLite WAL)
survives, and no data gap is introduced. Fix whatever doesn't recover.

## Files to change
- `dccd/application/scheduler.py` — only if `start()` doesn't fully reconstruct
  state from config on boot (it should `sync_streams` + `sync_intervals` from the
  loaded `AppConfig`). Verify a fresh `Scheduler.start()` re-arms everything from
  persisted config alone.
- `dccd/interfaces/cli/main.py` (`cmd_start`) / `dccd/interfaces/api/app.py`
  lifespan — only if the boot path doesn't reload config + RunsStore cleanly.
- Likely **no code change** if it already reconstructs from config — then this leaf
  is a verification + a regression test.

## Steps
1. Read `Scheduler.start`/`stop`, `sync_streams`, `sync_intervals` and the
   `cmd_start` / API lifespan boot path; confirm state is derived from
   `AppConfig` + `RunsStore` on every start (nothing held only in memory across
   the process boundary).
2. Identify any in-memory-only state that wouldn't survive a restart (e.g. an
   interval cadence or stream cursor not re-derived from config/disk). Fix it.
3. Confirm `RunsStore` opens the existing SQLite WAL and appends (doesn't
   truncate) on restart.

## Tests
- `dccd/tests/v3/test_application.py` (or a new `test_restart.py`): build a
  `Scheduler` from a config with a stream + an interval backfill, `start()`,
  `stop()`, then **construct a fresh `Scheduler` from the same config + same
  RunsStore path** and assert it re-arms the same stream/interval set and the runs
  history is intact (append, not reset).

## Verification on real data
- Real cycle on an **isolated store**: `dccd start` with a config that has one
  stream (e.g. binance trades) + one interval OHLC backfill; let it write a few
  Parquet rows + a couple of `RunsStore` rows; `kill -9`; `dccd start` again;
  confirm the stream reconnects, the interval re-arms, the runs DB kept its rows,
  and collection continues from the last point (no gap, no re-download — coverage
  manifest from Epic C handles the resume cursor).
- Back up the store dir before the cycle (per `data-e2e`).

## Closeout
- CHANGELOG (`Fixed`/`Added`): "Daemon reconstructs streams + interval backfills
  from config on restart; verified a kill→restart cycle resumes with no gap (#NN)".
- ADR: if a real persistence gap was found + fixed, record the choice (what state
  was moved from memory to config/disk and why).
- Status/roadmap: deferred to last leaf (06).
