---
plan: epic-a-remote-server/03-restart-safety
kind: leaf
status: planned
complexity: high
depends: [02]
parallel: false
branch: feat/restart-safety
pr: ""
---

# Persistence & restart safety (real reboot)

## Goal
Prove that after a **real machine reboot** of `dccd-testbox`, the systemd-managed
daemon: comes back up, **streams reconnect**, **interval backfills re-arm**,
`RunsStore` (SQLite WAL) **survives intact**, and collection resumes with **no data
gap** (no re-download, no lost window). Fix anything that doesn't recover; add a
regression test for the reconstruction logic.

## Context / ground truth (already verified by reading)
- Boot reconstructs from config: `cmd_start` builds store/runs/coverage/registry
  from `cfg` and calls `scheduler.start(cfg.all_job_specs())`
  (`dccd/interfaces/cli/main.py:159-197`).
- `Scheduler.start(specs)` re-creates a `_StreamWorker` per `supervised` spec and an
  interval loop per `interval`/`cron` spec from the specs alone
  (`dccd/application/scheduler.py:204-225`) — **no cross-process in-memory state**.
- Resume cursor is on disk: backfill resumes from `store.last_timestamp` else the
  **coverage manifest** `CoverageStore.get_max_ts` (Epic C) — so a restart resumes
  from the last collected point, not epoch.
- ⇒ restart safety is *structurally* present; this leaf is **verification +
  regression test**, with a code fix only if the reboot exposes a real gap (e.g. a
  cadence or cursor not actually re-derived from config/disk).

## Files to change
- Likely **none** in `scheduler.py` / `cli/main.py` if the reboot is clean. If a gap
  is found, fix the specific reconstruction (e.g. an interval cadence read from
  memory instead of `spec`, or `RunsStore` opened with a truncating mode).
- `dccd/tests/v3/test_restart.py` (new) — the regression test (below).

## Steps — real reboot on the box (depends on leaf 02's installed service)
1. **Arm a representative config** on the box: one **stream** (binance trades
   BTC/USDT, `supervised`) + one **interval** OHLC backfill (binance BTC/USDT 1h,
   short `every`). Restart the service to load it.
2. **Let it collect**, then snapshot pre-reboot state:
   ```
   ssh dccd-testbox 'sudo find /var/lib/dccd/data -name "*.parquet" -printf "%p %s\n";
     echo "--- runs ---"; sudo sqlite3 /var/lib/dccd/data/.dccd/runs.db \
       "select count(*), max(rowid) from runs;";
     echo "--- coverage ---"; sudo sqlite3 /var/lib/dccd/data/.dccd/coverage.db \
       "select dataset_id, max_ts from coverage;"'
   ```
   Record the max trade ts and the runs count.
3. **Reboot**: `ssh dccd-testbox 'sudo systemctl reboot'`; wait for the box to come
   back (`until ssh -o ConnectTimeout=5 dccd-testbox true; do sleep 3; done`).
4. **Confirm recovery** (service is `enable`d so it must auto-start):
   - `systemctl is-active dccd` → active; `systemctl show -p NRestarts dccd`.
   - `journalctl -u dccd --boot` shows a fresh "Daemon running" after the boot.
   - Stream reconnected: a `StreamSampleEvent`/new trades appear (watch
     `/api/events` or check new rows), and a new interval backfill run is logged.
5. **Prove no gap / no loss**:
   - `RunsStore` row count **≥** the pre-reboot count (append, not reset).
   - The trades Parquet has rows **after** the pre-reboot max ts, **contiguous** with
     it (no missing window); the coverage manifest `max_ts` only moved forward.
   - Re-run the snapshot from step 2 and diff.

## Tests (`dccd/tests/v3/test_restart.py`)
- Build a `Scheduler` from a config with one stream spec + one interval spec; `await
  start()`; `await stop()`. Construct a **fresh** `Scheduler` (new instances) from
  the **same** config + **same** `RunsStore` path; `await start()`; assert it
  re-arms the same stream/interval set (`_streams` keys, `_interval_loops` keys) and
  that `RunsStore` still returns the previously written runs (append, not truncate).
- Use a fake/in-memory adapter (as in `test_coverage.py`) — no network in unit tests.

## Acceptance criteria
- Box reboots; `dccd` service auto-active without manual action.
- Stream reconnects and a new interval backfill runs post-boot.
- `RunsStore` count ≥ pre-reboot; trades resume contiguous to the last ts (no gap,
  no re-download from epoch).
- `test_restart.py` passes; full `pytest` green.

## Verification on real data
The reboot cycle (steps 2–5) is the real verification — snapshot on-disk state,
reboot, compare. Back up nothing (throwaway). Capture both snapshots + the
`journalctl --boot` excerpt into the PR.

## Risks / rollback
- If the box doesn't come back (rare), it's physical/Tailscale-reachable — the user
  can power-cycle. Note the dependency on `systemctl enable` (done in leaf 02).
- A discovered gap means a real code fix — keep it minimal and ADR-document it.

## Closeout
- CHANGELOG (`Fixed`/`Added`): "Verified daemon survives a real reboot — streams +
  interval backfills re-arm from config, `RunsStore` survives, resume is gap-free;
  added `test_restart.py` (#NN)".
- ADR: only if a real persistence gap was found + fixed (what moved from memory to
  config/disk and why).
- Status/roadmap: deferred to leaf 06.
