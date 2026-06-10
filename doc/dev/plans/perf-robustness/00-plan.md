---
plan: perf-robustness
kind: global
status: executing
roadmap: "## Epic D — Performance & robustness (from the 2026-06-10 production audit)"
release_on_done: true
---

# Epic D — Performance & robustness

## Goal

The production audit of 2026-06-10 (arthurserver, 3.3.1, 50 jobs) found the
daemon pinned at **97.7 % CPU** with the UI unusable remotely (`/api/inventory`:
100 s for 10 KB over a 50-file / 32 MB store). A py-spy profile attributed ~96 %
of samples to `kraken.stream_orderbook` building the full book as pydantic
objects on **every WS delta** while `operations.stream` discards all but one
frame per `snapshot_interval`. Done = an idle collector daemon sits at < 10 %
CPU with 20 order-book streams, every `/api/*` endpoint answers in < 500 ms on
a populated store, WS subscription failures are loud, failing scheduled jobs
back off, and alerts don't spam.

Measured evidence and full findings: ADR journal entry (closeout of leaf 01) +
auto-memory `project-v33-perf-audit`.

## Decomposition

1. **orderbook-snapshot-throttle** — build `OrderBookSnapshot` only at capture
   time (`min_interval` pushed down into adapters); truncate Kraken/Bybit book
   state to the subscribed depth. Kills the CPU burn.
2. **inventory-footer-metadata** — `ParquetStore` metadata reads (`inventory`,
   `last_timestamp`, `missing_intervals`, `_is_year_complete`) from parquet
   footer stats instead of materialising the TS column; per-file mtime cache;
   API calls moved off-thread.
3. **ws-subscription-honesty** — declare valid order-book depths per capability
   and snap requested depths to them; surface WS subscription error/ACK frames
   instead of silently filtering them.
4. **scheduler-monitor-hygiene** — exponential backoff for failing interval
   jobs, startup jitter, HealthMonitor alert cooldown.
5. **ui-transport-efficiency** — GZip middleware, parallel dashboard fetches,
   saner poll intervals, RunsStore calls off-thread, honest stream end-state.

## Leaf checklist

- [x] 01 orderbook-snapshot-throttle — fix/orderbook-snapshot-throttle — medium
- [x] 02 inventory-footer-metadata — fix/inventory-footer-metadata — medium
- [ ] 03 ws-subscription-honesty — fix/ws-subscription-honesty — medium (depends on 01)
- [x] 04 scheduler-monitor-hygiene — fix/scheduler-monitor-hygiene — medium
- [ ] 05 ui-transport-efficiency — feat/ui-transport-efficiency — medium (depends on 02)

## Dependencies

- 03 depends on 01 (both rewrite the same adapter WS loops; serialise to avoid
  conflicts).
- 05 depends on 02 (both touch `interfaces/api/app.py`; 05's poll-interval
  choices assume inventory is cheap).
- 01, 02 and 04 are mutually independent (`parallel: true`) — disjoint files.

## Done criteria

- A local `dccd start` with ≥ 2 real Kraken + 1 Bybit order-book streams idles
  below ~10 % CPU (was: one Kraken stream alone saturated a core).
- `GET /api/inventory` on a populated store returns in < 500 ms and its JSON is
  byte-identical (same fields/values) to the pre-change implementation.
- A Kraken order-book job with `depth: 20` either snaps to a valid depth (and
  says so) or fails loudly — it never sits "live" writing nothing.
- A permanently failing interval job backs off (observed gaps grow) and fires
  exactly one alert at the threshold, then at most one per cooldown window.
- All five PRs merged into `develop`; roadmap Epic D section removed; suggest
  `/release`.
