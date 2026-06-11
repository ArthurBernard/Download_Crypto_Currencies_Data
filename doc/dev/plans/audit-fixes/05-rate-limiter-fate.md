---
plan: audit-fixes/05-rate-limiter-fate
kind: leaf
status: done
complexity: high
depends: [04]
parallel: false
branch: chore/rate-limiter-fate
pr: ""
---

# B5 — decide the RateLimiter's fate: wire it or delete it

## Goal

`transport/ratelimit.py` is exported but consumed nowhere; CLAUDE.md sells a
"token-bucket per exchange" that does not run. Today the only throttle is
reactive (429 + Retry-After in `http.py`), so a 50-job `run-all` hits every
exchange at full speed. Decide — with an ADR — between (a) wiring the
limiter into the outbound request path with sane per-exchange defaults, or
(b) deleting the module and correcting the docs. This is a judgement leaf:
investigate first, then implement the chosen option.

## Files to change

Option (a) — wire (the audit's lean, given `run-all` exists):
- `dccd/transport/http.py` or `dccd/sources/base.py` — single choke point:
  every adapter page fetch awaits `limiter.acquire(exchange)` before the
  request. The limiter instance lives in `service_factory.build_registry()`
  (one per process) so CLI/API/UI all inherit it.
- defaults: conservative public-REST rates per exchange (binance ~10/s,
  kraken ~1/s, coinbase ~3/s, others ~2/s — verify against current public
  docs before committing numbers); overridable via `AppConfig` if trivial,
  else hardcoded defaults + a follow-up roadmap line.
- CLAUDE.md + `doc/dev/02-architecture` (if it exists): description matches.

Option (b) — delete:
- remove `dccd/transport/ratelimit.py` + export + its tests; fix CLAUDE.md
  transport table; note in the ADR why reactive-only is acceptable.

## Steps

1. Re-read `transport/ratelimit.py`, `http.py` retry/429 handling, and how
   adapters issue requests post-leaf-04 (the operation-level context).
2. Check real 429 incidence on arthurserver logs (`ssh dccd-testbox`,
   journalctl) — evidence for/against proactive limiting.
3. Decide; write the ADR draft *first* (decision + why + rejected option).
4. Implement the chosen option; keep the change surgical.
5. `pytest`, `ruff`, `mypy`.

## Tests

- Option (a): unit test that two rapid `acquire("kraken")` calls space out
  per the configured rate (mock clock), and an integration test that a
  paginated fetch calls the limiter once per page.
- Option (b): test suite simply stays green after removal; grep guards
  nothing references `RateLimiter`.

## Verification on real data

- Option (a): isolated backfill against kraken (strictest public limits) of
  20+ pages → zero 429 in logs and total duration consistent with the
  configured rate. Then a small `run-all` style burst (3 concurrent
  backfills, same exchange) → still no 429.
- Option (b): n/a beyond the standard suite (document why in the report).

## Closeout

- CHANGELOG: option (a) `Added`: "Outbound token-bucket rate limiting per
  exchange on all REST fetches (#NN)" / option (b) `Removed`: "Unwired
  RateLimiter module; throttling remains reactive (429 + Retry-After) (#NN)"
- ADR: **mandatory** — the decision and the evidence.
- Status/roadmap: tick leaf in 00-plan; update CLAUDE.md transport row in
  the same PR.
