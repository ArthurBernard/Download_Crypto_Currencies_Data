---
name: release-gate
description: Full pre-release verification gate for dccd. Use before merging to develop/master, tagging a release, or whenever asked "is this releasable?", "run the full checks", "release gate", or "vérifie que tout passe avant la release". Runs the whole quality stack — unit tests, lint, types, docs build, and (opt-in) real-exchange E2E and the UI audit — and reports a single go/no-go.
---

# Release gate for dccd

A release is blocked unless **every** layer is green. Run them in cheap→expensive
order and stop reporting the first hard failure clearly (don't bury it).

## 1. Fast static + unit (always)

```bash
ruff check dccd/            # must say "All checks passed!"
mypy dccd/                  # must say "Success: no issues found"
pytest                      # all pass, network deselected
```
Notes:
- `mypy` is strict only on `domain/`; it assumes `python_version = 3.12` (the
  dev env ships Sphinx, which uses 3.12 syntax and otherwise makes mypy abort).
- `pytest` excludes `@pytest.mark.network` by default (`-m 'not network'`).

## 2. Docs build with zero warnings (always)

```bash
cd doc && make clean && make html 2>&1 | grep -ciE 'warning:'   # must be 0
```
A non-zero count usually means autodoc references a removed/renamed module — fix
the `.rst` or the docstring.

## 3. Real-exchange E2E (opt-in — needs network)

```bash
pytest -m network          # validates cursor pagination, symbol maps, OHLC round-trip live
```
Skip only if offline; say so explicitly in the report.

## 4. UI audit (opt-in — needs a browser)

Invoke the **ui-audit** skill (deep, hands-on UI verification). At minimum run
the smoke against an isolated instance:

```bash
# (isolated `dccd ui` on a temp config/data, then)
python doc/dev/ui_smoke.py http://127.0.0.1:<port>
```

## 5. Runtime resource gate (opt-in — needs network)

A green suite can still ship a daemon that burns a core (v3.3: order-book
snapshots built per WS delta → 97.7 % CPU on the production collector, remote
UI unusable). On an isolated config with 2-3 real stream jobs:

```bash
# after ~2 min of `dccd start` warm-up:
ps -o pcpu= -p <pid>     # steady-state must be < 10 %
curl -s -o /dev/null -w '%{time_total}\n' http://127.0.0.1:<port>/api/inventory
                         # must be < 0.5 s on a populated store
```

After upgrading a deployed server, also run `pip check` in its venv (a mixed
`polars`/`polars-lts-cpu` install breaks imports — seen at the 3.3 deploy) and
the same timed `curl` as a smoke test.

## 6. Release hygiene checks

- `git status` clean; no build artefacts / `pytest-of-*` / `doc/_build` tracked
  (they're gitignored — if any are staged, untrack them).
- `CHANGELOG.md` `[Unreleased]` reflects the actual changes (not a stale entry).
- Branch flow respected: never commit to `master`/`develop` directly; PR from a
  feature branch. `feat/refonte-v3` is the v3 integration branch.
- If tagging: version in `pyproject.toml` matches the intended tag.

## 7. Verdict

Report a table: layer → PASS/FAIL/SKIPPED(reason). **GO** only if tests, ruff,
mypy and docs are green and nothing in §6 is violated. Anything red ⇒ **NO-GO**,
name the blocker and the fix. Releasing (merge to master, tag, push) is the
user's call — never tag or merge to a protected branch without explicit approval.
