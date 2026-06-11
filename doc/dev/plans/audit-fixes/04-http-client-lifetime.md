---
plan: audit-fixes/04-http-client-lifetime
kind: leaf
status: done
complexity: medium
depends: []
parallel: false
branch: fix/http-client-lifetime
pr: ""
---

# P1/B4 — one httpx pool per operation, honest `Client.__aexit__`

## Goal

Every `fetch_*_page` does `async with self._http:` and the ref-count
(`_depth`) falls back to 0 between pages → a fresh `httpx.AsyncClient`
(TCP pool + TLS handshake) **per page**; a 500-page backfill performs 500
handshakes. Hold the client open for the whole operation. Also fix
`Client.__aexit__` (currently `pass`) so the public API context manager
keeps the pool alive across calls and closes it on exit, as its docstring
already promises.

## Files to change

- `dccd/sources/base.py` (or wherever adapters expose their
  `AsyncHTTPClient`) — expose a way to hold the transport open, e.g.
  `adapter.http()` returning the `AsyncHTTPClient` (it is already an async
  CM with ref-counting in `transport/http.py` `__aenter__`/`__aexit__`,
  lines ~63–78).
- `dccd/application/operations.py` — in `backfill()`, wrap the whole
  paginated section in `async with <adapter's client>:` so `_depth` stays
  ≥ 1 across pages; inner per-page `async with` become cheap ref-count
  bumps. Same for the orderbook snapshot branch (single page — harmless).
- `dccd/client.py` — `Client.__aenter__` enters the shared client(s) /
  `__aexit__` exits them, so Python-API users get one pool per `async with
  Client()` block. Audit found 2 prod runs failed with "Cannot send a
  request, as the client has been closed" (3.3.x) — the ref-count must make
  this impossible: add a concurrency test.

## Steps

1. Read `transport/http.py` fully; confirm the ref-count is asyncio-safe
   (audit: it is, `test_transport.py` covers concurrency) and that holding
   the context at operation level is the intended use.
2. Wire the operation-level context in `backfill()` (and `stream()` is WS —
   untouched).
3. Fix `Client.__aenter__`/`__aexit__`; keep the lazy-open behavior for
   non-context usage.
4. `pytest`, `ruff`, `mypy`.

## Tests

- `dccd/tests/v3/test_transport.py`: instrument `AsyncHTTPClient` (count
  `httpx.AsyncClient` constructions via monkeypatch) — a fake 5-page
  paginated backfill constructs **one** client, not five.
- Concurrent operations sharing one adapter still never see "client has
  been closed" (overlapping `async with` blocks).
- `Client` context: pool opened once on enter, closed on exit; reuse after
  exit reopens cleanly.

## Verification on real data

- Isolated store: real backfill of ~10+ pages (e.g. coinbase 1h OHLC over a
  long window — 300 candles/page) with httpx DEBUG logging or the
  construction counter: exactly one pool for the operation, and the data on
  disk matches the requested window (rows, bounds, no dup).

## Closeout

- CHANGELOG (`Fixed`): "Paginated backfills reuse one HTTP connection pool
  for the whole operation instead of opening a new TLS session per page;
  `Client` context manager now actually manages the shared pool (#NN)"
- ADR: none if the ref-count design holds; note one if semantics change.
- Status/roadmap: tick leaf in 00-plan.
