---
plan: audit-fixes
kind: global
status: executing
roadmap: "Epic E — Audit 2026-06-10 fixes (correctness, perf, prod hygiene)"
release_on_done: true
---

# Epic E — Audit 2026-06-10 fixes

## Goal

Close out the prioritized findings of the 2026-06-10 full-repo audit: two
production-confirmed run-lifecycle bugs (B6 zombie runs on no-capability
streams, B3 orphaned `running` rows after a crash), bounded data loss on
quiet streams (B2), outbound HTTP discipline (P1 client churn, B5 unwired
RateLimiter), production hardening on arthurserver (off-box backup, alerts,
systemd limits), an honest gap metric in the Data UI, the two biggest test
holes (CLI at 0 %, adapter parsing), and a small mechanical batch.

Out of scope, deliberately parked: P2 (append+compaction writes), P3
(filename pruning in `load()`), S1 (`/login` rate-limit), `run-all`
concurrency cap, B1 residual cancel races. They stay in the audit notes /
roadmap ideas until load or evidence demands them.

## Decomposition

1. **stream-nocapability-zombies** — B6: no run row before the capability
   check; supervisor abandons permanent errors and resets its backoff after
   a healthy period.
2. **runs-stale-purge** — B3: mark orphaned `running` runs `stale` at daemon
   boot (daemon boot only — not on CLI invocations).
3. **stream-time-flush** — B2: flush stream batches on a time interval too,
   and finally report real `rows_written` for stream runs.
4. **http-client-lifetime** — P1/B4: one httpx pool per operation instead of
   per page; make `Client.__aexit__` honest.
5. **rate-limiter-fate** — B5: wire the RateLimiter into the request path or
   delete it; either way the doc stops lying. ADR mandatory.
6. **prod-hardening** — arthurserver: rclone remote, alert webhook,
   `MemoryMax`/`TimeoutStopSec` in the unit; update the deploy how-tos.
7. **gap-metric-context** — UX: stop presenting trade-less minutes of
   illiquid pairs as "missing" data.
8. **cli-tests** — Typer `CliRunner` suite for `interfaces/cli/main.py` (0 %).
9. **adapter-fixtures** — recorded REST/WS payload fixtures for the
   low-coverage adapters + ws reconnect + ratelimit unit tests.
10. **minor-batch** — dynamic OpenAPI version, CI Sphinx job, local hygiene.

## Leaf checklist

- [x] 01 stream-nocapability-zombies — fix/stream-nocapability-zombies — medium
- [x] 02 runs-stale-purge — fix/runs-stale-purge — medium
- [x] 03 stream-time-flush — fix/stream-time-flush — medium (depends on 01)
- [x] 04 http-client-lifetime — fix/http-client-lifetime — medium
- [x] 05 rate-limiter-fate — chore/rate-limiter-fate — high (depends on 04)
- [x] 06 prod-hardening — chore/prod-hardening — medium
- [ ] 07 gap-metric-context — feat/gap-metric-context — medium
- [ ] 08 cli-tests — chore/cli-tests — medium (parallel)
- [ ] 09 adapter-fixtures — chore/adapter-fixtures — medium (parallel)
- [ ] 10 minor-batch — chore/audit-minor-batch — low

## Dependencies

- 03 depends on 01 (same lines of `operations.stream()` — avoid conflicts).
- 05 depends on 04 (limiter wiring sits on the corrected client lifetime).
- Everything else is independent; 08 and 09 are test-only and may run in
  parallel worktrees.

## Done criteria

- arthurserver runs.db: zero new zombie `running` rows after a simulated
  no-capability stream job; existing ~350 zombies marked `stale` after one
  daemon restart.
- A quiet-pair trades stream shows data on disk within ~2× the flush
  interval, and its run row reports non-zero `rows_written` after stop.
- A 500-page backfill opens one HTTP pool, not 500 (log/metric evidence).
- CLAUDE.md's transport description matches reality (RateLimiter wired or
  gone).
- Prod data syncs off-box on schedule; alert webhook fires on a forced
  failure; unit has explicit `MemoryMax` + `TimeoutStopSec`.
- Data UI no longer claims "85 % missing" for illiquid-but-complete pairs.
- `interfaces/cli/main.py` coverage > 0 % → meaningful (target ≥ 70 %);
  bitfinex/bitmex/coinbase adapters tested without network.
