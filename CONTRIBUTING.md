# Contributing to dccd

## Setup

```bash
git clone https://github.com/ArthurBernard/Download_Crypto_Currencies_Data.git
cd Download_Crypto_Currencies_Data
pip install -e ".[dev]"

# Activate the project git hooks (run once per clone)
git config core.hooksPath .githooks
```

## Git Flow

```
master          ← stable releases only (tagged vX.Y.Z, published to PyPI)
  └── develop   ← integration branch
        ├── feat/<topic>    new feature
        ├── fix/<topic>     bug fix
        ├── chore/<topic>   tooling, CI, deps, refactor
        └── docs/<topic>    documentation only
```

**Rules:**
- Never commit directly to `master` — always go through `develop` via a PR.
- Never commit directly to `develop` for non-trivial work — use a feature branch.
- Branch off `develop`, not `master`.
- `develop` → `master` happens only at release time (version bump + tag).

**Branch naming:** `feat/`, `fix/`, `chore/`, `docs/` + short kebab-case description.

**Commit style:** [Conventional Commits](https://www.conventionalcommits.org/)

```
feat: add Bybit historical downloader
fix: correct timestamp overflow in date_to_TS
chore: migrate CI to GitHub Actions
docs: update README with Coinbase example
```

Do not add `Co-Authored-By` trailers to commits — this is a personal repo.

## Running tests

```bash
# Full suite (doctests + unit tests)
pytest

# Single test file
pytest dccd/tests/test_binance.py -v

# With coverage
pytest --cov=dccd --cov-report=term-missing
```

## Linting

```bash
ruff check dccd/
```

Both must pass before opening a PR.

## Release process (maintainer only)

1. All planned work merged into `develop`, CI green.
2. Bump `version` in `pyproject.toml`.
3. Update `CHANGELOG.md` — move `[Unreleased]` to the new version.
4. Open PR `chore/release-X.Y.Z` into `develop`, then `develop` → `master`.
5. After merge to master: `git tag vX.Y.Z && git push origin vX.Y.Z`.
