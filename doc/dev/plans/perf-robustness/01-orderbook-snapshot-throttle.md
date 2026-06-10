---
plan: perf-robustness/01-orderbook-snapshot-throttle
kind: leaf
status: done
complexity: medium
depends: []
parallel: true
branch: fix/orderbook-snapshot-throttle
pr: "#XX"
---

# Throttle order-book snapshot construction upstream (kills the 98 % CPU burn)

## Goal

Adapters currently build a full `OrderBookSnapshot` (pydantic: one
`OrderBookLevel` per price level, sorted) on **every** WS frame; the consumer
(`operations.stream`) discards all but one per `snapshot_interval` (default
60 s). Move the throttle *upstream*: apply deltas to cheap dict state on every
frame, but **construct pydantic objects only when a capture is due**. Measured
on arthurserver: ~96 % of daemon CPU samples were in `kraken.stream_orderbook`
lines 343–345 (`pydantic __init__`).

## Files to change

- `dccd/sources/base.py` — `OrderBookLive.stream_orderbook` protocol gains a
  keyword-only param: `def stream_orderbook(self, symbol, depth, *,
  min_interval: float = 0.0)`. `0.0` = legacy behaviour (yield every frame).
- `dccd/sources/kraken.py` — `KrakenSource.stream_orderbook` forwards
  `min_interval` to `_KrakenWS`; `_KrakenWS.stream_orderbook`:
  1. keep applying snapshot/delta frames to `state_bids`/`state_asks` dicts
     exactly as today;
  2. **before** any `OrderBookLevel`/`OrderBookSnapshot` construction, check
     `time.monotonic() - last_emit < min_interval` → `continue`;
  3. at emit time, sort once, **truncate both sides to the subscribed depth**
     (`self._param`) for the snapshot AND prune `state_bids`/`state_asks` to
     those same top-`depth` levels (Kraken WS v2 contract says the client
     truncates after updates; pruning at emit bounds stale-level retention to
     one interval — note this in a comment);
  4. emitted snapshots are full state → `is_snapshot=True` always.
- `dccd/sources/bybit.py` — same treatment in `_BybitWS.stream_orderbook`
  (lines ~199–235): delta state in dicts, throttle check before construction,
  truncate/prune to depth at emit.
- `dccd/sources/binance.py`, `dccd/sources/okx.py`, `dccd/sources/bitmex.py` —
  these receive push-snapshots (top-5/10/20; OKX `books5` pushes at ~10 Hz).
  Accept the new `min_interval` kwarg and skip frames (after the cheap
  `json.loads` + channel check, before building any pydantic object) until the
  interval has elapsed. For the ones built on `parse_message`/`stream()`
  (binance, okx, bitmex), the simplest compliant change is a small wrapper
  generator in the adapter's `stream_orderbook` that tracks `last_emit` and
  drops early frames *before* parse where possible, else after parse — the
  invariant to honour is: **no pydantic construction for a frame that will be
  dropped**. Restructure `parse_message` into a raw-loop generator if needed
  (kraken/bybit already use `stream_raw`).
- `dccd/sources/coinbase.py`, `dccd/sources/bitfinex.py` — no live order book
  (capability not declared); only update signatures if they stub the protocol.
- `dccd/application/operations.py` — `stream()` order-book branch: pass
  `min_interval=snapshot_interval` to `adapter.stream_orderbook(...)` and
  **remove** the local `if time.time() - last_save < snapshot_interval:
  continue` throttle — save every yielded snapshot (the adapter now owns the
  cadence). Keep `_emit_sample` on save.

## Steps

1. Change the protocol signature in `base.py` (keyword-only, defaulted — no
   call-site breakage).
2. Rework `_KrakenWS.stream_orderbook` per above; verify the truncation uses
   `self._param` (the subscribed depth).
3. Rework `_BybitWS.stream_orderbook` identically.
4. Add the throttle wrapper to binance/okx/bitmex `stream_orderbook`.
5. Update `operations.stream` (pass-through + remove downstream throttle).
6. Run `pytest`, `ruff check dccd/`, `mypy dccd/`.

## Tests

- `dccd/tests/v3/test_sources.py` (or a new `test_orderbook_throttle.py`):
  - feed a scripted sequence of Kraken book frames (1 snapshot + N deltas)
    through `_KrakenWS.stream_orderbook` with a monkeypatched
    `time.monotonic`; assert exactly one snapshot is yielded per
    `min_interval` window, and zero `OrderBookLevel` construction happens for
    skipped frames (e.g. monkeypatch/spy the class or count via a wrapped
    constructor).
  - assert emitted snapshots are truncated to `depth` levels per side and that
    a delta-removed level (qty 0) is gone.
  - `min_interval=0.0` keeps legacy per-frame behaviour (regression guard for
    existing tests).
  - same scripted-frames test for Bybit.
- Existing protocol-compliance tests must still pass unmodified.

## Verification on real data

- On an **isolated store** (`data-e2e` discipline): run a real
  `operations.stream` against live Kraken (BTC/USD order book,
  `snapshot_interval=10`, depth 25) for ~60 s. Assert: ~6 snapshots landed in
  Parquet, each with ≤ 25 levels per side, ts monotonic, no crossed book
  (max bid < min ask).
- CPU check: run `dccd start` locally with 2 Kraken + 1 Bybit order-book
  streams for 2 minutes; `ps -o pcpu= -p <pid>` must read **< 10 %**
  (pre-change: a single Kraken stream saturated a core). Record both numbers
  in the PR body.

## Closeout

- CHANGELOG (`Fixed`): "Order-book WS adapters built the full book as pydantic
  objects on every delta (~98 % CPU on a live collector, starving the event
  loop and the remote UI); snapshots are now constructed only at capture time
  (`min_interval` pushed down into the adapters) and Kraken/Bybit book state is
  truncated to the subscribed depth. (#NN)"
- ADR: throttle lives in the adapter (not the consumer) because the cost to
  kill is the *construction*, which only the adapter can skip; `min_interval=0`
  preserves the per-frame contract for any other consumer.
- Status/roadmap: tick leaf 01 in `00-plan.md`; roadmap line D1 removed by
  `/finish-task` of the last leaf only.
