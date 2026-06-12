---
plan: hardening-backlog-2026-06/02-duplicate-run-guard
kind: leaf
status: executing
complexity: medium
depends: []
parallel: false
branch: fix/duplicate-run-guard
pr: ""
---

# Manual triggers must not start duplicate concurrent runs

## Goal

`POST /api/backfill`, `/api/jobs/run` and `/api/jobs/run-all` happily start a
second run for a spec that is already being backfilled. Benign for data
(store locks + dedup; shared rate limiter) but it wastes exchange requests
and confuses runs/progress. Make the trigger idempotent: when
`active_runs()` already holds a run for the same spec id, return the
existing `run_id` (HTTP 200, `status: "already-running"`) instead of
starting a new one.

## Files to change

- `dccd/interfaces/api/app.py` — small helper near the endpoints, e.g.
  `_active_run_for(request, spec_id) -> str | None`, wrapping
  `await asyncio.to_thread(_runs(request).active_runs)` and returning the
  `run_id` of the first row whose `spec_id` matches. Use it in:
  - `start_backfill` (`POST /api/backfill`): after building `spec`, if an
    active run exists → `{"run_id": <existing>, "status": "already-running"}`.
  - `run_job_now` (`POST /api/jobs/run`): same check before
    `_run_backfill_tracked` → add `"job_id"` to the response as today.
  - `run_all_backfill_jobs` (`POST /api/jobs/run-all`): skip specs with an
    active run; report them in a separate `"already_running"` list
    (`[{run_id, job_id}, …]`) alongside the started ones, and count only the
    actually-started in `"started"`.
- `dccd/tests/v3/test_api.py` — see Tests.

## Steps

1. Add the helper + the three call sites (keep the responses backward
   compatible: same keys as today plus the new `status`/`already_running`
   fields).
2. Tests below; `pytest` + `ruff check dccd/`.

## Tests

`dccd/tests/v3/test_api.py` (follow the existing TestClient + tmp data_path
patterns; an "active" run = `runs_store.create_run(...)` without finishing,
with `spec_id` equal to what `JobSpec.make_id("backfill", target)` yields for
the request):

- `test_backfill_duplicate_returns_existing_run` — pre-create a `running`
  row for the spec id of the request; `POST /api/backfill` → 200,
  `status == "already-running"`, `run_id` equals the pre-created one, and no
  new run row was added.
- `test_jobs_run_duplicate_returns_existing_run` — same through
  `POST /api/jobs/run` with a configured job.
- `test_run_all_skips_active_jobs` — two configured backfill jobs, one with
  an active run: `POST /api/jobs/run-all` starts only the other
  (`started == 1`) and lists the busy one under `already_running`.

## Verification on real data

- Isolated store + a real `dccd ui`/TestClient app wired to a slow real
  backfill is overkill here; instead verify against the live server *after
  deploy is not required*: locally run the API (TestClient) with a fake
  adapter whose `fetch_ohlc_page` sleeps, `POST /api/backfill` twice for the
  same spec, and confirm the second response returns the first `run_id`
  while runs.db contains exactly one `running` row for the spec. (This is
  an API-behaviour leaf, not a data-path leaf — the runs.db assertion *is*
  the on-disk verification.)

## Closeout

- CHANGELOG `Fixed`: "manual backfill triggers (`POST /api/backfill`,
  `/api/jobs/run`, `/api/jobs/run-all`) are idempotent: a spec already being
  backfilled returns the existing `run_id` (`status: already-running`) /
  is skipped by run-all, instead of starting a duplicate concurrent run
  (#NN)"
- ADR: none — behaviour choice is recorded in the roadmap line and the
  response contract; mention "return existing id (not 409)" in the PR body.
- Status/roadmap: remove the duplicate-run-guard bullet from the Hardening
  backlog.
