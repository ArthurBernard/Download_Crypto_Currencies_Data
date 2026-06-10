---
plan: perf-robustness/03-ws-subscription-honesty
kind: leaf
status: planned
complexity: medium
depends: [01]
parallel: false
branch: fix/ws-subscription-honesty
pr: ""
---

# Honest WS subscriptions: valid depths + loud subscription failures

## Goal

The production config had Kraken order-book jobs with `depth: 20` and
`depth: 50` — Kraken WS v2 only accepts {10, 25, 100, 500, 1000} — and the
adapters filter every non-data frame, so a rejected subscription leaves a
stream "live" that never writes anything, forever and silently. Declare valid
depths in the capability, snap requested depths to them, and surface
subscription error frames as failures.

## Files to change

- `dccd/domain/capability.py` — add `depths: list[int] | None = None`
  (discrete valid order-book depths; `None` = unconstrained; keep the existing
  `max_depth` untouched). Update docstring attribute list.
- `dccd/sources/kraken.py` — declare `depths=[10, 25, 100, 500, 1000]` on the
  ws/live orderbook capability. In `_KrakenWS.stream_raw` consumers (the
  book/ohlc/trade loops) detect Kraken v2 method-ACK frames:
  `{"method": "subscribe", "success": false, "error": "..."}` → raise
  `RuntimeError(f"kraken subscription failed: {error}")`.
- `dccd/sources/bybit.py` — declare valid depths for the spot book channel
  (`depths=[1, 50, 200]` — verify against current Bybit v5 spot docs before
  hardcoding; use what the docs say, not this line). Detect
  `{"op": "subscribe", "success": false, "ret_msg": ...}` → raise.
- `dccd/sources/okx.py` — `books5` is fixed top-5: declare `depths=[5]`.
  Detect `{"event": "error", "msg": ..., "code": ...}` → raise.
- `dccd/sources/binance.py` — already clamps to {5, 10, 20}; declare
  `depths=[5, 10, 20]` so the engine knows.
- `dccd/sources/bitmex.py` — `orderBook10` fixed top-10: `depths=[10]`.
  Detect `{"status": 4xx, "error": ...}` subscription errors → raise.
- `dccd/application/operations.py` — in `stream()`'s order-book branch, after
  fetching `cap = adapter.capability_for(...)`: if `cap.depths` and the
  requested depth is not in it, **snap to the smallest valid depth ≥
  requested** (else the largest valid) and emit a warning log event
  (`_emit_log`-equivalent via `events.log(..., level="warning")`) naming both
  values — existing configs keep working, loudly.

## Steps

1. Add the `Capability.depths` field + docstring.
2. Declare `depths` on the five adapters' ws/live orderbook capabilities
   (check each exchange's current public docs for the true list; adjust the
   values above if the docs disagree — the **docs win**).
3. Implement the snap + warning in `operations.stream`.
4. Add subscription-error detection to each adapter's WS loop. A raised error
   propagates to `_StreamWorker._run_forever`, which already logs and retries
   with backoff — no scheduler change needed. Make sure the error message
   names the exchange, channel and pair.
5. `pytest`, `ruff check dccd/`, `mypy dccd/`.

## Tests

- `dccd/tests/v3/test_sources.py` — capability declarations: each adapter with
  a live orderbook capability declares a non-empty `depths`.
- `test_application.py` (or new file) — `stream()` with a fake adapter whose
  cap has `depths=[10, 25]`: requesting depth 20 calls
  `adapter.stream_orderbook` with 25 and logs a warning; requesting 25 passes
  through silently; requesting 9999 snaps to 25.
- Per-adapter: feed a scripted subscription-error frame into the WS loop
  (kraken: `{"method":"subscribe","success":false,"error":"Subscription
  depth not supported"}`) and assert it raises with the exchange and reason in
  the message — not silently filtered.

## Verification on real data

- Against **live Kraken**: start a real order-book stream with `depth=20` on
  an isolated store; assert the warning event fires, the effective
  subscription is depth 25, and snapshots actually land in Parquet (this
  exact case wrote nothing on arthurserver).
- Negative path: subscribe to a nonsense Kraken symbol; assert the stream run
  is recorded `failed` with the Kraken error text within one reconnect cycle
  (visible in `/api/runs`), not "live with no data".

## Closeout

- CHANGELOG (`Fixed`): "Order-book stream jobs with a depth the exchange
  doesn't support (e.g. Kraken `depth: 20`) silently produced no data: valid
  depths are now declared per capability and requests snap to the nearest
  valid value with a warning; WS subscription rejections raise (and surface in
  runs) instead of being filtered out. (#NN)"
- ADR: snap-with-warning over hard-fail — existing deployed configs (this one
  included) must keep collecting after upgrade; honesty is preserved by the
  warning + the declared capability.
- Status/roadmap: tick leaf 03 in `00-plan.md`.
