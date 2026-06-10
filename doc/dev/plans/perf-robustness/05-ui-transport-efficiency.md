---
plan: perf-robustness/05-ui-transport-efficiency
kind: leaf
status: done
complexity: medium
depends: [02]
parallel: false
branch: feat/ui-transport-efficiency
pr: "#XX"
---

# HTTP/UI transport efficiency: gzip, parallel fetches, saner polling, off-thread SQLite

## Goal

Remote (Tailscale/TLS) UI pages pay uncompressed JSON, serial fetches and
aggressive polling. Add gzip, parallelise the dashboard's fetches, slow the
polls that exist only as fallbacks, move RunsStore SQLite calls off the event
loop, and report an unexpectedly-ended stream as `failed` rather than
`cancelled`.

## Files to change

- `dccd/interfaces/api/app.py` —
  - `from fastapi.middleware.gzip import GZipMiddleware` +
    `app.add_middleware(GZipMiddleware, minimum_size=1024)` (SSE is exempt
    automatically: streaming responses with unknown length are passed through
    by Starlette's GZip only when buffering — verify with a test asserting
    `/api/events` still streams, see Tests).
  - wrap the synchronous RunsStore calls in `asyncio.to_thread`:
    `GET /api/runs` (`list_runs`), `GET /api/backfill/{run_id}` (`get_run`),
    and the `list_runs` call inside `GET /api/storage/sync`.
- `dccd/interfaces/ui/templates/dashboard.html` — `load()` fetches runs,
  streams and inventory **in parallel** (`Promise.all`, same pattern as
  live.html); poll interval 8 s → 15 s.
- `dccd/interfaces/ui/templates/storage.html` — `setInterval(loadSync, 10000)`
  → 30 s (the page is a status view; "Sync now" already polls itself).
- `dccd/interfaces/ui/templates/live.html` — keep the 8 s `loadAll` for
  jobs/streams state, but fetch `/api/inventory` only on initial load and on
  SSE `status` events (it seeds liveness; live samples come over SSE) — split
  the `Promise.all` accordingly and keep `INV` cached between polls.
- `dccd/application/operations.py` — `stream()`: when the async generator ends
  **without** `stop_event` set (WS generator exhausted unexpectedly), finish
  the run as `failed` with error `"stream ended unexpectedly"`; keep
  `cancelled` only for an actual stop request. (The supervisor restarts it
  either way; this only fixes the recorded state shown in Logs/Runs.)

## Steps

1. API changes (gzip + to_thread).
2. Template changes (three files).
3. `stream()` end-state fix.
4. `pytest`, `ruff check dccd/`, `mypy dccd/`; run `doc/dev/ui_smoke.py`
   against a local `dccd ui` (all steps must stay green).

## Tests

- `dccd/tests/v3/test_api.py`:
  - a `GET /api/inventory` response with `Accept-Encoding: gzip` on a
    populated-enough store (> 1 KB JSON) carries `content-encoding: gzip`;
  - `/api/events` SSE still yields the heartbeat within the test timeout with
    the middleware installed (regression: gzip must not buffer the stream);
  - existing endpoint tests stay green (to_thread is behaviour-neutral).
- `test_application.py`: a fake stream adapter whose generator returns after N
  records (no stop requested) → run recorded `failed` with
  "stream ended unexpectedly"; with `stop_event` set → still `cancelled`.

## Verification on real data

- Run `dccd ui` against a real populated store; from another machine (or
  `curl --compressed` locally): `/api/inventory` transfer size shrinks ≥ 5×
  vs uncompressed (`%{size_download}` with/without `Accept-Encoding`).
- Load Dashboard, Live, Storage in a headless browser (`ui-audit` discipline,
  or `doc/dev/ui_smoke.py`): no JS errors, liveness dots still seed and tick,
  network tab shows inventory fetched once on Live load and not on every 8 s
  poll.

## Closeout

- CHANGELOG (`Changed`): "Remote-friendly UI transport: gzip on API responses,
  dashboard fetches parallelised, lighter poll cadences (inventory no longer
  re-fetched every 8 s by Live), and runs-store reads moved off the event
  loop." + (`Fixed`): "A live stream that ended unexpectedly was recorded
  `cancelled`; it is now `failed` with an explicit error. (#NN)"
- ADR: none — mechanical; note in the PR body that SSE-pushed inventory
  invalidation was considered and deferred (footer-stats cache made polling
  cheap enough).
- Status/roadmap: tick leaf 05 in `00-plan.md`; this is the last leaf — apply
  the epic closeout: remove the Epic D section from `doc/dev/07-roadmap.md`,
  update `doc/dev/06-status.md`, archive the tree, suggest `/release`.
