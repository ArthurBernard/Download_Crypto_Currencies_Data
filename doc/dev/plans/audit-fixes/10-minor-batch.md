---
plan: audit-fixes/10-minor-batch
kind: leaf
status: done
complexity: low
depends: []
parallel: false
branch: chore/audit-minor-batch
pr: ""
---

# Minor batch — dynamic OpenAPI version, CI Sphinx job, local hygiene

## Goal

Close the audit's mechanical leftovers in one small chore PR, plus two
local-machine cleanups that need no PR.

## Files to change

- `dccd/interfaces/api/app.py` line ~232 — `FastAPI(title="dccd v3",
  version="3.0.0", ...)` → use the package version
  (`importlib.metadata.version("dccd")` or `dccd.__version__`), so /docs
  stops claiming 3.0.0 forever.
- `.github/workflows/` (the existing CI workflow) — add a docs job:
  install `.[dev]` + docs deps, run `sphinx-build` and **fail on any
  warning** (`-W` or grep, matching the repo's "0 warnings" rule). Keep it
  a separate job so the test matrix is untouched.

Local ops (no commit, run at closeout and report):
- purge the 21 local branches already merged into develop
  (`git branch --merged develop`, delete all but develop/master).
- pyenv global 3.12.13: `pip uninstall dccd` (broken v2 ghost that shadows
  the real install outside the project dir).
- dccd_env: `pip install -e ".[dev]"` to refresh the stale 3.0.0 metadata.

## Steps

1. Version: single-line change + a test.
2. CI: add the docs job; trigger it on the PR itself to prove it passes
   (and that it *would* fail — locally verify `sphinx-build -W` catches an
   injected warning, then revert the injection).
3. Local ops; list what was deleted in the leaf report.
4. `pytest`, `ruff`, `mypy`.

## Tests

- `dccd/tests/v3/test_api.py`: `app.version` (or `/openapi.json` `info.version`)
  equals `importlib.metadata.version("dccd")` — never a hardcoded literal.

## Verification on real data

- n/a (no data path). CI run on the PR is the verification for the docs job.

## Closeout

- CHANGELOG (`Fixed`): "OpenAPI reports the real package version (#NN)";
  (`Added`): "CI job enforcing the Sphinx 0-warning rule (#NN)"
- ADR: none — mechanical.
- Status/roadmap: tick leaf in 00-plan; this being the last leaf is fine —
  whichever leaf closes last removes the Epic E roadmap section.
