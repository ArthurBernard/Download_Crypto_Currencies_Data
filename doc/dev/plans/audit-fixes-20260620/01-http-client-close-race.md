---
plan: audit-fixes-20260620/01-http-client-close-race
kind: leaf
status: planned
complexity: medium
depends: []
parallel: true
branch: fix/http-client-close-race
pr: ""
---

# Close the shared-HTTP-client refcount race

## Goal
`AsyncHTTPClient.__aexit__` must not leave the dying client reachable while it is
being closed. Null `self._client` (and reset depth) *before* awaiting `aclose()`,
so a concurrent `__aenter__` during that await creates a fresh client instead of
reusing the closing one.

## Background (the race)
`__aexit__` currently does, when depth hits 0:
```python
self._depth = 0
await self._client.aclose()   # yields control
self._client = None           # only nulled AFTER the await
```
While `aclose()` awaits, another scheduled job's `__aenter__` sees
`self._client is not None`, skips creation, bumps depth to 1, and then `.get()`s a
closing/closed client → `httpx` raises "Cannot send a request, as the client has
been closed." Observed ~1×/day on arthurserver across binance/okx/kraken OHLC
backfills (each backs off 3600 s and self-heals). The refcount was meant to make
the shared client concurrency-safe (a documented invariant), but the
null-after-await ordering leaves a hole.

## Files to change
- `dccd/transport/http.py` — in `__aexit__`, capture the client into a local,
  null `self._client` and reset `self._depth = 0`, *then* `await client.aclose()`.
  Update the inline comment to state the ordering matters (null-before-await closes
  the re-entry window).

## Steps
1. Rewrite the `__aexit__` body to:
   ```python
   self._depth -= 1
   if self._depth <= 0 and self._client is not None:
       self._depth = 0
       client = self._client
       self._client = None      # null BEFORE awaiting: a concurrent __aenter__
       await client.aclose()    # during aclose() then builds a FRESH client
   ```
2. Keep `__aenter__` unchanged (it already creates a client when `self._client is
   None`).

## Tests
- `dccd/tests/v3/test_transport.py` — add an async regression test that
  deterministically reproduces the race with a fake client whose `aclose()` yields
  (`await asyncio.sleep(0)`/event) so a concurrent `__aenter__` interleaves:
  - monkeypatch `httpx.AsyncClient` with a `FakeClient` that records `closed` and
    whose `aclose()` awaits before setting `closed=True`;
  - enter once (depth 1, client = fake1); start `__aexit__()` as a task; yield;
    concurrently `__aenter__()` again;
  - assert the client observed after the concurrent enter is **not None and not
    closed** (i.e. a fresh client), and that fake1 still gets closed.
  - With the old ordering this fails (concurrent enter reuses fake1, which is then
    closed and nulled); with the fix it passes.

## Verification on real data
- After release + deploy: tail the server journal for ≥1 backfill cycle and
  confirm no new `Cannot send a request, as the client has been closed.` lines
  appear, and the previously-stale kraken DOGE-BTC OHLC resumes (fresh `max_ts`
  within the hourly cadence). Pre-deploy, the deterministic unit test is the
  primary evidence (the race is timing-dependent on live data).

## Closeout
- CHANGELOG (`Fixed`): "`AsyncHTTPClient.__aexit__` nulls the shared client
  reference before awaiting `aclose()`, closing a refcount race where a concurrent
  request reused the closing client (`Cannot send a request, as the client has
  been closed`). (#NN)"
- ADR: yes — record the null-before-await ordering as the chosen fix over
  alternatives (a lock around enter/exit; never closing the client). Concurrency
  invariant.
- Status/roadmap: no roadmap line; deferred to last leaf (none to remove).
