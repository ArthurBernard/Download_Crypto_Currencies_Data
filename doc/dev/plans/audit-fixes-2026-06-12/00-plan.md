---
plan: audit-fixes-2026-06-12
kind: global
status: executing
roadmap: "## Audit fixes (2026-06-12)"
release_on_done: true
---

# Audit fixes — 2026-06-12 production audit

## Goal

Fix the two bugs found auditing the production collector (arthurserver) on
2026-06-12. Both are small, independent, and verified root-caused:

1. **OKX OHLC loses one bar per pagination page.** `fetch_ohlc_page` passes
   `before = start_ms`, but OKX v5 `before`/`after` cursors are **exclusive** —
   the bar exactly at each pagination-window start is never returned. Observed on
   the server: 431 one-minute gaps per OKX pair, spaced exactly 100 minutes
   (= the 100-bar page), all created by the 2026-06-10 deep backfill. It also
   explains why hourly OKX runs write 60 rows where Binance writes 61.
2. **`dccd start` boot race marks live stream runs stale.** `cmd_start` calls
   `scheduler.start()` (each stream worker creates its `running` row in runs.db)
   *before* `uvicorn` triggers the FastAPI lifespan, which then calls
   `RunsStore.mark_stale_running()` and sweeps those legit rows to `stale`
   ("orphaned by daemon restart") one second after creation. Observed on the
   server: zero `running` rows while 20 streams collect; Dashboard "Active now"
   never shows streams under `dccd start`.

## Decomposition

1. **okx-ohlc-inclusive-window-start** — make the OKX OHLC window start
   inclusive (`before = start_ms - 1`) + boundary regression test.
2. **boot-orphan-sweep-order** — sweep orphaned runs in `cmd_start` *before*
   `scheduler.start()`; skip the sweep in the lifespan when a scheduler is
   injected.

## Leaf checklist

- [x] 01 okx-ohlc-inclusive-window-start — fix/okx-ohlc-boundary-bar — medium
- [ ] 02 boot-orphan-sweep-order — fix/boot-orphan-sweep — medium

## Dependencies

- None — 01 and 02 are independent (disjoint files).

## Done criteria

- Both leaf PRs merged into `develop`; `pytest` green; invariants intact.
- Leaf 01 verified on real data: an isolated OKX 1m backfill spanning several
  100-bar page boundaries lands with `missing_rows == 0`.
- Leaf 02 verified live: a locally started `dccd start` daemon shows its stream
  run rows in state `running` (not `stale`) while alive.
- **Ops follow-up (post-release/deploy, not a repo leaf):** re-backfill the five
  OKX pairs on arthurserver from 2026-05-11 so the 431-gap-per-pair history is
  repaired (dedup makes this safe); confirm inventory `missing_rows == 0` and
  that a daemon restart leaves 20 `running` stream rows.
