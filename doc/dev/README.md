# Developer & process docs

Internal docs for contributors — planning, retrospectives, and the testing
playbook. (End-user docs live in `doc/source/`; the project README, CHANGELOG,
CLAUDE.md and CONTRIBUTING.md stay at the repo root.)

## Testing & methodology
- [`v3-testing-and-findings.md`](v3-testing-and-findings.md) — testing layers and
  how to run each, the recipes (isolated instance, challenge-on-real-data,
  back-up-before-mutation), the catalogue of bugs found and **how** each was
  found, and the invariants to preserve.
- [`ui_smoke.py`](ui_smoke.py) — runnable headless-browser UI audit
  (`python doc/dev/ui_smoke.py http://127.0.0.1:8137`).

## v3 remediation (post-rewrite)
- [`RETROSPECTIVE-v3.md`](RETROSPECTIVE-v3.md) — honest analysis of the v3
  execution (what shipped vs the plan, regressions, gaps).
- [`PLAN-v3-remediation.md`](PLAN-v3-remediation.md) — the remediation plan and
  its outcome (M1–M3 done; release pending).

## Original v3 refonte planning
- [`REFONTE.md`](REFONTE.md), [`REFONTE-plan.md`](REFONTE-plan.md),
  [`REFONTE-noyau.md`](REFONTE-noyau.md), [`REFONTE-capacites.md`](REFONTE-capacites.md)
  — the hexagonal-rewrite design and phase plan (P0–P8).
- [`TODO.legacy.md`](TODO.legacy.md) — archived pre-v3 backlog.

> The live operational backlog is `TODO.md` at the repo root (gitignored, local).
> Related skills: `.claude/skills/{ui-audit,release-gate,data-e2e}`.
