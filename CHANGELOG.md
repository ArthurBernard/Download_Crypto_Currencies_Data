# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `dccd/continuous_dl/binance.py` — `DownloadBinanceData` streaming trades and order book via Binance combined WebSocket streams (#20)
- `dccd/continuous_dl/kraken.py` — `DownloadKrakenData` streaming trades, order book, and OHLCV via Kraken WebSocket v2 (#20)
- `dccd/continuous_dl/okx.py` — `DownloadOKXData` streaming trades, order book, and candles via OKX WebSocket v5 (#20)
- `get_trades_*`, `get_orderbook_*`, `get_data_*` high-level helpers for Binance, Kraken, and OKX (#20)
- `dccd/tests/test_binance_ws.py`, `test_kraken_ws.py`, `test_okx_ws.py` — 34 new tests for the new WS modules (#20)
- `README.rst` and `doc/source/index.rst` — exchange support matrix table (REST/WS × data type) (#20)
- `dccd/tests/test_websocket.py`, `test_bitfinex.py`, `test_bitmex.py`, `test_bybit_ws.py` — tests for `continuous_dl` and `BasisWebSocket`; coverage lifted from excluded to 82% overall (#12)
- `dccd/tests/test_histo_dl.py` — tests for `_get_last_date` (xlsx, csv, parquet, empty directory) (#12)
- `pyproject.toml` — `pytest-asyncio>=0.23` added to dev dependencies (#12)

### Changed

- `dccd/histo_dl/exchange.py` — `_get_last_date` now reads `.csv` and `.parquet` files in addition to `.xlsx` instead of falling back to 2012-01-01 (#12)
- `dccd/histo_dl/exchange.py` — completed numpydoc docstrings for `_get_last_date`, `_set_by_period`, `_name_file`, `_excel_format`, `_sort_data`, `set_hierarchy` (#12)
- `dccd/tools/io.py` — documented `driver`, `username`, `password`, `host`, `port` parameters of `save_as_sql` (#12)
- `dccd/continuous_dl/exchange.py` — documented `time_step=None` tick-by-tick behaviour in `ContinuousDownloader` (#12)
- `dccd/continuous_dl/bitfinex.py` — resolved all inline TODOs, added full type annotations, removed dead `__main__` block (#12)
- `dccd/continuous_dl/bitmex.py` — resolved all inline TODOs, added full type annotations, fixed undefined `pair` variable in `get_data_bitmex`, removed dead `__main__` block (#12)
- `pyproject.toml` — removed `bitfinex` and `bitmex` from mypy `ignore_errors` override; lifted `continuous_dl/*` and `tools/websocket.py` from coverage omit (#12)

## [2.0.2] - 2026-05-15

### Changed

- `README.rst` — added PyPI status, docstring coverage, and downloads badges

## [2.0.1] - 2026-05-14

### Changed

- Docstrings `See Also` updated in `FromBinance`, `FromKraken`, `FromCoinbase` — replaced defunct `FromGDax`/`FromPoloniex` with `FromBybit`/`FromOKX`
- `doc/source/index.rst` — exchange lists updated (Bybit, OKX added); all exchange RST pages added to toctree (previously orphaned)
- `dccd/__init__.py` module docstring — exchange list updated
- `pyproject.toml` — added `Documentation` and `Changelog` project URLs
- `README.rst` — added exchange support table, output format section, multi-exchange Quick start examples
- `examples/historical_downloader.py` — rewritten with modern API (Binance + Parquet)

## [2.0.0] - 2026-05-14

### Added

- `pyproject.toml` (PEP 517/518) — replaces `setup.py` (#5)
- GitHub Actions CI (`.github/workflows/ci.yml`) — matrix Python 3.10/3.11/3.12/3.13, jobs `test` and `lint` (#5)
- `dccd/histo_dl/coinbase.py` — `FromCoinbase` class replacing the defunct GDAX module (#5)
- `.githooks/pre-push` — Git Flow enforcement (#5)
- `CONTRIBUTING.md` — development setup, Git Flow, commit conventions (#5)
- `CHANGELOG.md` (#5)
- `.pre-commit-config.yaml` — hooks `ruff` (lint + fix) et `ruff-format` (#7)
- `dccd/tests/test_date_time.py`, `test_io.py`, `test_process_data.py` — couverture ≥ 80 % (#8)
- `.github/workflows/badges.yml` — badge couverture docstrings via `interrogate` (#8)
- `dccd/histo_dl/bybit.py` — `FromBybit` : téléchargement historique Bybit v5 REST (#9)
- `dccd/continuous_dl/bybit.py` — `DownloadBybitData` : stream WebSocket Bybit v5 (#9)
- `dccd/histo_dl/okx.py` — `FromOKX` : téléchargement historique OKX v5 REST (#9)
- `dccd/models.py` — `OHLCBar`, `Trade`, `OrderBookEntry` : validation pydantic des réponses API (#9)
- `IODataBase.save_as_parquet` — format Parquet via pyarrow (optionnel `dccd[io]`) (#9)
- `IODataBase.save_as_polars` — format Polars, Parquet sous le capot (optionnel `dccd[io]`) (#9)
- `ImportDataCryptoCurrencies.get_data(format='polars')` — retourne un `pl.DataFrame` (#9)
- `dccd/tools/date_time.py`, `tools/io.py`, `histo_dl/exchange.py`, `continuous_dl/exchange.py`, `tools/websocket.py` — type hints complets (#10)
- `.github/workflows/release.yml` — publication automatique PyPI + GitHub Release sur tag `v*` via OIDC (#10)

### Changed

- **Breaking:** minimum Python version is now 3.10 (dropped 3.5–3.9) (#5)
- **Breaking:** minimum dependency versions bumped — `pandas>=2.0`, `SQLAlchemy>=2.0`, `numpy>=1.26`, `requests>=2.28`, `websockets>=12.0`, `scipy>=1.10` (#5)
- Replaced `xlrd` + `xlsxwriter` with `openpyxl` for Excel I/O (#5)
- `dccd/histo_dl/exchange.py`: `df.append()` → `pd.concat()`, `ffill()`, `openpyxl` engine (#5)
- `dccd/tools/io.py`: `SQLAlchemy URL()` → `URL.create()`, `df.append()` → `pd.concat()` (#5)
- Version now managed via `importlib.metadata` (#5)
- `dccd/tools/websocket.py`: `asyncio.get_event_loop().run_until_complete()` → `asyncio.run()` (#7)
- `dccd/tests/conftest.py`: fixtures `tmp_data_path` + mocks HTTP — les tests ne font plus d'appels réseau (#7)
- `doc/source/conf.py` : thème scipy → furo, extensions modernisées (#8)
- `dccd/histo_dl/binance.py` : API v1 → v3 (#9)
- `dccd/histo_dl/exchange.py` : `_fetch()` avec retry tenacity sur HTTP 429 (#9)
- `dccd/tools/websocket.py` : reconnexion automatique avec `max_retries` et `retry_delay` (#9)
- `print()` remplacés par `logging` dans `exchange.py`, `binance.py`, `date_time.py` (#9)
- `pyproject.toml` : `mypy>=1.0` + `pandas-stubs>=2.0` dans `dev`, section `[tool.mypy]` (#10)
- `.github/workflows/ci.yml` : étape `mypy dccd/` ajoutée dans le job `lint` (#10)
- `dccd/tools/websocket.py` : arguments mutables `conn={}` / `subs={}` corrigés en `None` (#10)

### Fixed

- `dccd/tools/io.py` : logique CSV inversée dans `save_as_csv` — le fichier existant était écrasé au lieu d'être appendé (#8)

### Removed

- **Breaking:** `FromPoloniex` and `dccd/histo_dl/poloniex.py` — Poloniex exchange shut down in 2024 (#5)
- **Breaking:** `FromGDax` and `dccd/histo_dl/gdax.py` — GDAX API endpoint defunct; replaced by `FromCoinbase` (#5)
- `setup.py`, `tox.ini`, `requirements.txt`, `doc-requirements.txt`, `.travis.yml` (#5)
