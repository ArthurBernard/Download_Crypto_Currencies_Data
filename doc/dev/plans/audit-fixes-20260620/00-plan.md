---
plan: audit-fixes-20260620
kind: global
status: planning
roadmap: "none — surfaced by the 2026-06-20 arthurserver health audit (not a roadmap item)"
release_on_done: true
---

# Audit fixes (2026-06-20 server audit)

## Goal
Two small, independent robustness fixes surfaced by the 2026-06-20 production
audit of arthurserver. Both are real but low-frequency and self-healing; neither
threatens data. "Done" = both shipped, a `3.6.3` patch released, and the server
redeployed so the two error signatures stop appearing in the journal.

The audit also produced **operational cleanup** (purge 7 orphan datasets with no
feeding job; repopulate the empty UI-token convenience file) — that is server-side
housekeeping, done directly out-of-band, *not* part of this code plan.

## Decomposition
1. **http-client-close-race** — `AsyncHTTPClient.__aexit__` awaits `aclose()`
   *before* nulling `self._client`, so during that await a concurrent
   `__aenter__` reuses the dying client → "Cannot send a request, as the client
   has been closed." (~1 scheduled backfill failure/day, across binance/okx/kraken).
2. **login-nonascii-token** — `POST /login` calls `secrets.compare_digest` on two
   `str`s; a non-ASCII submitted token makes it raise `TypeError`, returning a 500
   instead of a clean invalid-login (scanner/bot hits on the tailnet port).

## Leaf checklist
- [x] 01 http-client-close-race — fix/http-client-close-race — medium
- [ ] 02 login-nonascii-token — fix/login-nonascii-token — low

## Dependencies
- None. 01 and 02 touch disjoint files (`transport/http.py` vs
  `interfaces/api/app.py`) and could run in parallel; executed serially here (safe
  default).

## Done criteria
- `transport/http.py` nulls the shared client reference before awaiting `aclose()`;
  a deterministic regression test reproduces the old race and passes with the fix.
- `POST /login` no longer raises on a non-ASCII token (returns the login error
  page); a TestClient regression test covers it.
- `pytest` + `ruff check dccd/` green; both fixes shipped in `v3.6.3` and the
  server redeployed (`pip install --no-deps --upgrade dccd==3.6.3`, polars-lts-cpu
  preserved).
