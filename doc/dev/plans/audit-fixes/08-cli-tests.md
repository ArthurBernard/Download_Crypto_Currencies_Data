---
plan: audit-fixes/08-cli-tests
kind: leaf
status: done
complexity: medium
depends: []
parallel: true
branch: chore/cli-tests
pr: ""
---

# Tests — CLI suite (`interfaces/cli/main.py` is at 0 %)

## Goal

The Typer CLI (146 stmts) is tested nowhere; the audit's only dynamic check
was manual. Add a `CliRunner`-based suite covering every command's happy
path and the obvious failure modes, without network.

## Files to change

- `dccd/tests/v3/test_cli.py` (new) — `typer.testing.CliRunner` against
  `dccd.interfaces.cli.main.app`.
- Possibly tiny refactors in `interfaces/cli/main.py` ONLY if a command is
  untestable as written (e.g. hardcoded paths) — keep them minimal and
  behavior-preserving.

## Steps

1. Inventory the commands (`dccd --help` / reading `main.py`): expect
   `validate`, `backfill`, `stream`, `read`/`status`, `inventory`, `ui`,
   `start`, jobs-related, version.
2. Fixtures: a tmp config YAML + tmp data dir; monkeypatch
   `service_factory` builders to in-memory/tmp stores; stub the registry
   with a fake adapter so `backfill` runs end-to-end offline (write real
   rows through the real ParquetStore — the CLI→operation→store chain is
   the point).
3. Cover: exit codes (0 on success, non-zero on bad exchange/config),
   `--help` for each command, `validate` on a bad YAML, `inventory` output
   on a seeded store, `backfill` writing rows via the fake adapter.
4. Do NOT spawn servers (`ui`/`start`): assert they're wired by checking
   the command exists and bails correctly on a bad config (`--help` level),
   not by binding ports.
5. `pytest` (suite must stay < ~10 s), `ruff`, `mypy`.

## Tests

This leaf *is* tests. Target: `interfaces/cli/main.py` ≥ 70 % coverage
(from 0 %), overall coverage strictly up.

## Verification on real data

- Not a data-path change. One manual sanity: `dccd backfill` real run
  against binance on an isolated store still behaves identically (no CLI
  refactor regression).

## Closeout

- CHANGELOG (`Added`): "CLI test suite (CliRunner) — commands covered
  offline end-to-end (#NN)"
- ADR: none — mechanical.
- Status/roadmap: tick leaf; update test counts in `06-status.md`.
