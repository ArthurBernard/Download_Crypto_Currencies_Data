# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Changed

### Fixed

### Removed

## [2.0.0] - 2026-05-12

### Added

- `pyproject.toml` (PEP 517/518) — replaces `setup.py`
- GitHub Actions CI (`.github/workflows/ci.yml`) — matrix Python 3.10/3.11/3.12/3.13, jobs `test` and `lint`
- `dccd/histo_dl/coinbase.py` — `FromCoinbase` class replacing the defunct GDAX module; uses the public Coinbase Exchange API (`api.exchange.coinbase.com`)
- `.githooks/pre-push` — Git Flow enforcement (block direct push to `master`)
- `CONTRIBUTING.md` — development setup, Git Flow, commit conventions
- `CHANGELOG.md`

### Changed

- **Breaking:** minimum Python version is now 3.10 (dropped 3.5, 3.6, 3.7, 3.8, 3.9)
- **Breaking:** minimum dependency versions bumped — `pandas>=2.0`, `SQLAlchemy>=2.0`, `numpy>=1.26`, `requests>=2.28`, `websockets>=12.0`, `scipy>=1.10`
- Replaced `xlrd` + `xlsxwriter` with `openpyxl` for Excel I/O
- `dccd/histo_dl/exchange.py`: `df.append()` → `pd.concat()`, `groupby(axis=0)` → `groupby()`, `fillna(method='pad')` → `ffill()`, `is 'string'` → `== 'string'`, `xlsxwriter` engine → `openpyxl`
- `dccd/tools/io.py`: `SQLAlchemy URL()` → `URL.create()`, `df.append()` → `pd.concat()`
- Version now managed via `importlib.metadata` (no more generated `dccd/version.py`)
- Pytest configuration moved from `tox.ini` to `pyproject.toml`
- `.readthedocs.yml` updated to Python 3.12

### Removed

- **Breaking:** `FromPoloniex` and `dccd/histo_dl/poloniex.py` — Poloniex exchange shut down in 2024
- **Breaking:** `FromGDax` and `dccd/histo_dl/gdax.py` — GDAX API endpoint defunct; replaced by `FromCoinbase`
- `setup.py`, `tox.ini`, `requirements.txt`, `doc-requirements.txt`, `.travis.yml`
