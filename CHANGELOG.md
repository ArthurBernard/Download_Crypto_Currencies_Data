# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added (v3 remediation, pre-3.0.0)

- Reworked web UI split by concern: a read-only enriched **Inventory** (data
  freshness, OHLC gap detection, on-disk size, per-exchange totals) and two
  collection pages — **Historical** and **Live** — each with data-type tabs and
  per-exchange accordions. Jobs are created, edited (first date) and deleted
  inline on the page; the Live page shows a real-time liveness indicator (last
  trade/quote + age) fed by a throttled stream heartbeat over SSE. (#76)
- Job CRUD over the API: `POST /api/jobs/create|delete|update`, backed by
  `AppConfig.add_job`/`remove_job`/`update_job_start` (persisted to `config.yml`).
- `ParquetStore.inventory()` now reports on-disk `bytes` and, for OHLC,
  `expected_rows`/`missing_rows` (gap detection) at no extra read cost.
- `EventBus` fan-out to multiple SSE consumers and a `StreamSampleEvent`
  liveness sample emitted (throttled) by `operations.stream`.
- UI polish: nav reorganised into `Collect ▾`/`System ▾` dropdowns; **Inventory**
  renamed **Data** (`/inventory`→`/data`) with data-type tabs; reworked Live
  liveness — seeded from the last on-disk data point so a page refresh shows
  freshness immediately (no "waiting…"), span-aware dot, a freshness label that
  is a live relative "N min ago" counter under 24h and an absolute date beyond,
  and no noise age for fresh trades, with client-side number formatting;
  order-book cadence (`snapshot_interval`) shown and settable;
  Storage shows on-disk sizes; Dashboard adds a KPI bar and clearer sections;
  Logs reoriented around recent runs with human run labels. The Config page no
  longer duplicates job management (jobs live on Historical/Live; raw edit via
  its JSON tab). `GET /api/jobs` now returns `start`/`every`/`snapshot_interval`/
  `depth`. (#76)
- Cursor-based trades pagination: the engine now follows each adapter's opaque
  cursor until a window is drained, instead of advancing by a fixed time window.
  Fixes silent loss of >95% of trades on every liquid pair (all exchanges).
- Complete v2→v3 Parquet migration (`dccd migrate`): renames `quoteVolume`→
  `quote_volume`, drops `weightedAverage`, adds a `trades` column, rescales
  second timestamps to nanoseconds — schema-aware and idempotent.
- Bearer auth on `/api/*` when `settings.ui_auth_token` is set, with a `?token=`
  fallback for Server-Sent Events; `settings.ui_allow_origins` for opt-in CORS.
- Public async `Client.read()` and `Client.stream()`; `Client` wires adapters
  via `service_factory` (single source of truth).
- Network-marked end-to-end tests (`pytest -m network`) validating pagination
  against live exchange APIs.

### Fixed

- Data loss on merge: writing into an existing legacy v2 Parquet file no longer
  silently overwrites it; existing rows are canonicalised and preserved.
- Provenance is now actually written into the Parquet footer (was computed but
  dropped).
- Custom ISO start date for backfill no longer raises (`JobParams.start`).
- Historical *first date* edit no longer reverts on reload: `GET /api/jobs` was
  not returning `start`, so the UI reset the field after every refresh. (#76)
- Live order-book streams reported a crossed/incorrect best bid-ask: the WS
  adapters emitted unmerged diff levels. binance/okx/bitmex now use full
  snapshot channels (`@depth<N>`, `books5`, `orderBook10`) and bybit
  reconstructs full state from snapshot+deltas (like kraken); best bid/ask is
  computed defensively (`max` bid / `min` ask). (#76)
- Order-book Live liveness was incoherent with its cadence: it sampled the WS
  every second while only one snapshot per ``snapshot_interval`` is captured. The
  liveness sample is now emitted when a snapshot is actually saved, so its age
  counts up to the interval and resets (matching the "Δ Ns" cadence). (#76)
- `dccd inventory` no longer crashes on OHLC datasets.
- Streams with no real implementation (Coinbase OHLC/order book, Bitfinex order
  book) are rejected with `NoCapability` instead of "running" with zero output.
- `history="recent"` exchanges (Kraken OHLC) are clamped + warned instead of
  silently returning wrong deep history.
- `mypy dccd/` runs and passes again (it had been aborting on the dev Sphinx).

### Changed / Removed

- Honest OHLC fidelity: Coinbase `quote_volume` is null (was a fabricated
  `close×volume`); Kraken now fills its native trade count.
- Removed the dead `parallel` backfill flag, the unused `Page` model and the
  unused bundled `htmx.min.js`.

> v3 is a full hexagonal rewrite. It **removes** the v2 daemon web UI shipped in
> 2.4.0 (`dccd/daemon/*`) and replaces it with `dccd/interfaces/` (api/cli/ui).

## [2.4.0] - 2026-06-04

### Added

- `dccd/daemon/api.py` — web UI and JSON API (FastAPI + Jinja2 + htmx): a thin
  HTTP layer over the existing daemon modules exposing dashboard (live health
  metrics), inventory (stored data coverage), jobs (histo/stream list + add/remove
  + live backfill progress), logs (tail), config (view/validate/save the YAML),
  and storage (rclone status + manual sync). JSON-only API (`/api/*`) with
  dumb-shell templates, so the front-end can be swapped without touching the API.
  Optional Bearer-token auth via `settings.ui_auth_token`
- `dccd/daemon/cli.py` — `dccd ui`: serve the web UI standalone; the UI is also
  started automatically (background thread) by `dccd start` when the `[ui]` extra
  is installed
- `dccd/daemon/config.py` — `SettingsConfig.ui_host`, `ui_port`, `ui_auth_token`:
  web UI bind address, port, and optional auth token
- `dccd/daemon/backfill.py` — `progress_callback` and `stop_event` on
  `_BackfillBase.run()` / `run_backfill()`: let the UI report live progress and
  cancel a running backfill (defaults keep CLI behaviour unchanged)
- `dccd/daemon/stream_manager.py` — `SyncService` writes
  `{local_path}/.dccd/last_sync.json` after each successful remote push, so the UI
  can display the last sync time
- `pyproject.toml` — new optional extra `[ui]` (`fastapi`, `uvicorn[standard]`,
  `jinja2`); install with `pip install dccd[daemon,ui]`

## [2.3.3] - 2026-05-31

### Added


- `doc/source/` — complete Sphinx documentation overhaul: redesigned homepage
  with sphinx-design cards, captioned toctrees (Getting Started / Data Collection /
  Reference), new pages (`installation`, `quickstart`, `changelog`, `cli`,
  `configuration`, `models`, `storage`, `tools`, per-exchange histo/continuous pages),
  adaptive light/dark logo and favicon, sticky top navbar with PyPI/GitHub/Fynance
  links, hero header with inline logo+title, responsive layout (#59, #61)
- `README.md` — converted from RST to Markdown; inline logo+title header with
  `<picture>` for light/dark mode switching; badges on two rows (#60)

## [2.3.2] - 2026-05-25

### Added

- `dccd/daemon/cli.py` — `dccd status --json`: emit raw metrics as a JSON object on stdout, suitable for piping into Grafana / jq (#53)
- `dccd/daemon/config.py` — `HistoJob.max_retries` (int, 1–10, default 3) and `HistoJob.retry_delay` (float ≥ 0, default 2.0): per-job retry configuration for transient network errors; delay is exponential (`retry_delay * 2^(attempt-1)`) (#53)
- `dccd/daemon/config.py` — `resolve_config_path()` and `DEFAULT_CONFIG_PATH`: CLI commands now fall back to `$XDG_CONFIG_HOME/dccd/config.yml` (default `~/.config/dccd/config.yml`) when no `--config` option is provided and `./config.yml` does not exist (#49)
- `dccd/daemon/cli.py` — `dccd inventory`: scans `data_path` and prints a table of all stored OHLC, trades, and orderbook data with date range, row count, and gap count per series; uses Polars for fast columnar reads (#50)
- `dccd/daemon/cli.py` — `dccd remove --exchange X --pair Y --span N`: removes a pair from a histo_job (or the whole job if it was the last pair) and re-validates the config before writing (#50)

### Changed

- `dccd/storage.py`, `dccd/histo_dl/exchange.py`, `dccd/daemon/backfill.py`, `dccd/process_data.py`, `dccd/daemon/stream_manager.py` — replace pandas with polars throughout; `DataStore.save/load` accept/return `pl.DataFrame`; `get_data()` defaults to `format='polars'`; `set_marketdepth` returns a flat long-format `pl.DataFrame`; stream savers write parquet via `DataStore`; `pandas` removed from core dependencies (#52)
- `dccd/daemon/backfill.py` — backfill progress bar now shows the current window date (`YYYY-MM-DD → YYYY-MM-DD`) instead of a raw window count (`n win`); makes it easy to see which period is being downloaded at a glance (#48)

### Fixed

- `dccd/histo_dl/exchange.py` — `_sort_data` no longer raises `ColumnNotFoundError: "date"` when the exchange API returns an empty candle list; the polars migration (PR #52) had re-introduced a variant of the empty-data crash from v2.3.1; now returns early with an empty `self.df` (#54)

## [2.3.1] - 2026-05-24

### Fixed
- `dccd/storage.py` — `DataStore.missing_intervals` now detects the gap **before** the first saved row when the requested `start` predates `file_min`; previously only the trailing gap (after `file_max`) was returned, causing `dccd backfill --start <early-date>` to silently skip all historical data before the first existing candle (#46)
- `dccd/histo_dl/coinbase.py` — raise `RuntimeError` when Coinbase returns HTTP 200 with a JSON dict (e.g. `{"message": "..."}` for near-future windows) instead of silently iterating dict keys and crashing with `ValueError` (#45)
- `dccd/histo_dl/coinbase.py` — additional guard: raise `RuntimeError` when Coinbase returns a JSON list whose first element is not itself a list/tuple (e.g. `["message"]`); previously caused `float("m")` `ValueError` (#45)
- `dccd/histo_dl/exchange.py` — `_sort_data` no longer raises `KeyError: 'TS'` when the API returns empty data; returns early with an empty `self.df` so the backfill skips the window cleanly (#45)
- `dccd/histo_dl/exchange.py` — `_sort_data` strips any candle at or beyond `self.end` before merging; exchanges with inclusive endpoint semantics (Coinbase) no longer cause `_advance` to overshoot by one span per window, preventing drift that accumulated into near-future requests (#45)
- `dccd/histo_dl/okx.py` — raise `RuntimeError` when OKX response code is not `"0"`, letting the backfill retry/skip logic handle API-level errors (#45)
- `dccd/histo_dl/okx.py` — switch `_import_data` from `/market/candles` to `/market/history-candles`; the former only serves the last ~24 h of 1-minute bars and silently returns empty data for older windows (#45)

## [2.3.0] - 2026-05-22

### Added

- `dccd/storage.py` — `DataStore.is_period_complete(year)`: checks whether an annual parquet file contains all expected candles; `DataStore.missing_intervals(start, end)`: gap-detection — complete past years are skipped, incomplete years resume from the last saved row (#41)
- `dccd/daemon/backfill.py` — `_BackfillBase.run()` now iterates over `DataStore.missing_intervals()` instead of a single sliding window from `last_saved`; complete years are never re-downloaded (#41)
- `dccd/storage.py` — new `DataStore` class: unified read/write interface for OHLC, trades, and orderbook; `save(df)` (merge-on-TS, annual OHLC / daily trades+orderbook), `load(start, end)`, `existing_periods()`, `last_timestamp()` (#39)
- `dccd/tools/date_time.py` — `span_label(span)` converts seconds to short directory labels (``'1m'``, ``'1h'``, ``'1d'``…); `_SPAN_LABEL` mapping exported (#39)
- `doc/source/storage.rst` — Sphinx page for `DataStore` with directory layout examples (#39)

### Changed

- `dccd collect` (formerly `dccd run`) — renamed to clarify the distinction: `collect` = one incremental batch, `backfill` = full historical download with gap detection, `start` = continuous daemon (#41)
- New storage arborescence: ``{data_path}/{exchange}/ohlc/{pair}/{span}/YYYY.parquet``, ``…/trades/{pair}/YYYY-MM-DD.parquet``, ``…/orderbook/{pair}/YYYY-MM-DD.parquet`` — replaces the old ``{Exchange}/Data/Clean_Data/{per}/{pair}/`` layout (#39)
- `dccd/histo_dl/exchange.py` — `save()`, `_get_last_date()`, `save_trades()`, `save_orderbook()` now delegate to `DataStore`; removed `last_df`, `_set_by_period`, `_name_file`, `_excel_format`; removed unused `set_hierarchy()` (#39, #41)
- `dccd/histo_dl/{binance,bybit,coinbase,okx}.py` — removed `full_path` overrides (base class sets the correct path via `DataStore`) (#39)
- `dccd/daemon/backfill.py`, `scheduler.py` — removed `by_period` parameter; `save()` call simplified (#39)
- `dccd/daemon/stream_manager.py` — WebSocket save path now built from `DataStore.directory` (#39)
- `dccd/daemon/config.py` — `HistoJob.by_period` field removed; granularity is automatic (#39)

- `dccd/histo_dl/exchange.py` — `save()` now supports `form='parquet'`; previously only `'xlsx'` and `'csv'` were handled (#35)
- `config.yml` — ready-to-use daemon config for minutely OHLC + real-time orderbook/trades on Binance, Kraken, and Bybit (#35)
- `dccd/daemon/backfill.py` — `OHLCBackfill` and `KrakenBackfill` strategy classes with shared retry/progress/save loop; `make_job()` factory; `run_backfill()` orchestrator; tqdm progress bars and optional `--parallel` execution (#38)
- `dccd/daemon/cli.py` — `dccd backfill` command: reads all job definitions from config, supports `--exchange` / `--pairs` filters, `--start`, `--parallel`, and `--dry-run` flags (#38)
- `dccd/daemon/config.py` — `SettingsConfig` with `data_path` and `timezone` fields; `CollectorConfig.settings` propagates `data_path` to `StorageConfig.local_path` when not set explicitly (#38)

### Removed

- `scripts/backfill.py` — replaced by `dccd backfill` CLI command and `dccd.daemon.backfill` module (#38)

### Fixed

- `dccd/histo_dl/exchange.py` — `save(form='parquet')` was silently ignored (logged a warning instead of writing the file) (#35)
- `dccd/histo_dl/exchange.py` — `_sort_data()` crashed with a ValueError when the API returned fewer candles than the expected window size; index is now derived from actual data (#36)
- `dccd/histo_dl/exchange.py` — `by_period='M'` produced minute-level file names (strftime `%M`) instead of year-month; added `_PERIOD_FMT` mapping so `'M'` → `'%Y-%m'` (#36)
- `dccd/histo_dl/exchange.py` — `self.end` now reflects the last candle timestamp so window-loop callers advance correctly (was stuck at `now` for Kraken) (#36)
- `dccd/histo_dl/binance.py` — missing `limit=1000` parameter caused Binance to return only 500 candles per request (#36)
- `dccd/histo_dl/bybit.py` — `limit` was 200; raised to 1 000 to match the API maximum (#36)
- `dccd/histo_dl/exchange.py` — `_sort_data()` dropped the minute just before a window boundary when the last trade arrived ≥2 spans early; grid now uses `self.end` directly as the exclusive stop (#36)

## [2.2.0] - 2026-05-17

### Added

- `dccd/histo_dl/exchange.py` — `import_trades(start, end)` and `import_orderbook(depth)` public methods on `ImportDataCryptoCurrencies`; `_sort_trades` / `_sort_orderbook` helpers validate via Pydantic, sort and deduplicate; `trades_df` / `orderbook_df` attributes; `save_trades` / `save_orderbook` save helpers (#31)
- `dccd/histo_dl/{binance,kraken,bybit,okx,coinbase}.py` — `_import_trades(start, end)` and `_import_orderbook(depth)` implemented for all five exchanges; Binance and Kraken support full history via paginated endpoints; Bybit (≤ 1 000) and Coinbase (≤ 100) return recent-only snapshots (#31)
- `dccd/models.py` — `Trade.tid` made optional (`int | None`); `OrderBookEntry` gains required `side` field (`'bid'` or `'ask'`) and `count` made optional (`int | None`) (#31)
- `dccd/daemon/health.py` — `HealthMonitor`: rotating log handler (10 MB × 5 files), per-job metrics JSON, and optional Slack/Discord webhook alerts on consecutive failures; `JobMetrics` dataclass (#30)
- `dccd/daemon/cli.py` — `dccd` CLI (`validate`, `run`, `start`, `status`, `add` commands) via typer; `[project.scripts]` entrypoint; `typer>=0.12` added to the `daemon` extra (#30)
- `dccd/daemon/stream_manager.py` — `StreamManager` (one thread per `(exchange, pair)`, auto-restart on crash) and `SyncService` (periodic rclone push to all remotes, decoupled from collection) (#26)
- `dccd/daemon/config.py` — declarative YAML config with Pydantic v2: `CollectorConfig`, `HistoJob`, `StreamJob`, `StorageConfig`, `AlertConfig`, `RemoteConfig`, `load_config()` (#25)
- `dccd/daemon/storage.py` — `RemoteStorage.push()` via rclone; supports multiple remotes and root-path sync (#25, #26)
- `dccd/daemon/scheduler.py` — `build_histo_scheduler()` (APScheduler 3.x), `run_histo_job()`, `run_once()` (#25)
- `examples/config.example.yml` — annotated reference config for the daemon (#25)
- `examples/daemon_example.py` — programmatic daemon example in 6 steps (#30)
- `pyproject.toml` — `[daemon]` optional extra (`pyyaml`, `apscheduler`, `typer`) (#25, #30)

### Changed

- `dccd/daemon/scheduler.py` — `run_histo_job`, `build_histo_scheduler`, `run_once` accept an optional `health: HealthMonitor` parameter (#30)
- `dccd/daemon/stream_manager.py` — `StreamManager.__init__` accepts optional `health: HealthMonitor`; `_run_forever` records success/failure on each iteration (#30)
- `dccd/daemon/config.py` — `StorageConfig.remote` replaced by `remotes: list[RemoteConfig]` and `sync_interval: int` (#26)
- `dccd/histo_dl/{binance,coinbase,bybit,okx,kraken}.py` — `format_pair(crypto, fiat)` extracted as a static method, independently testable (#29)
- `dccd/continuous_dl/exchange.py` — unified `__call__`, `_push_trades`, `_push_book_updates`, `_get_book_state`, `_restore_book_state` in base class; separate `set_trades_saver` / `set_book_saver`; crash-recovery checkpoint; `snapshot_ts` injected into every snapshot payload (#28, #29)

## [2.1.0] - 2026-05-15

### Added

- `dccd/tests/test_binance.py`, `test_kraken.py`, `test_bybit.py`, `test_okx.py`, `test_coinbase.py` — REST error-scenario tests: HTTP 500 and malformed response for every exchange (#22)
- `dccd/continuous_dl/binance.py` — `DownloadBinanceData` streaming trades and order book via Binance combined WebSocket streams (#20)
- `dccd/continuous_dl/kraken.py` — `DownloadKrakenData` streaming trades, order book, and OHLCV via Kraken WebSocket v2 (#20)
- `dccd/continuous_dl/okx.py` — `DownloadOKXData` streaming trades, order book, and candles via OKX WebSocket v5 (#20)
- `get_trades_*`, `get_orderbook_*`, `get_data_*` high-level helpers for Binance, Kraken, and OKX (#20)
- `dccd/tests/test_binance_ws.py`, `test_kraken_ws.py`, `test_okx_ws.py` — 34 new tests for the new WS modules (#20)
- `README.rst` and `doc/source/index.rst` — exchange support matrix table (REST/WS × data type) (#20)
- `dccd/tests/test_websocket.py`, `test_bitfinex.py`, `test_bitmex.py`, `test_bybit_ws.py` — tests for `continuous_dl` and `BasisWebSocket`; coverage lifted from excluded to 82% overall (#12)
- `dccd/tests/test_histo_dl.py` — tests for `_get_last_date` (xlsx, csv, parquet, empty directory) (#12)
- `pyproject.toml` — `pytest-asyncio>=0.23` added to dev dependencies (#12)

### Fixed

- `dccd/tools/date_time.py` — `span_to_str` and `str_to_span` now cover all spans supported by the exchanges: 180 s (3 m), 900 s (15 m), 14400 s (4 h), 21600 s (6 h), 28800 s (8 h), 43200 s (12 h), 259200 s (3 d), 1296000 s (15 d), 2592000 s (1 M); previously any span outside the original 7 values returned `None` and silently broke the save path (#21)
- `dccd/histo_dl/kraken.py` — `import_data` now raises `UserWarning` when `end` is passed, as the Kraken OHLC API does not support a custom end date and silently ignored the parameter (#21)

### Changed

- `dccd/continuous_dl/exchange.py` — `get_parser()` now raises `KeyError` on unknown key instead of falling back to the removed debug parser; `_loop()` awaits `is_connect` instead of `_data` (#22)
- `dccd/continuous_dl/bitmex.py`, `dccd/continuous_dl/bybit.py` — added numpydoc docstrings on `_parser_trades()` and `_parser_book()` (#22)
- `dccd/histo_dl/exchange.py` — `ImportDataCryptoCurrencies` docstring updated: `See Also` lists all five exchanges; `platform` parameter documents all supported values; fixed typos (#22)
- `dccd/histo_dl/exchange.py` — `ImportDataCryptoCurrencies` now inherits from `ABC` and `_import_data` is decorated with `@abstractmethod`, preventing accidental instantiation of the base class (#21)
- `dccd/histo_dl/binance.py`, `coinbase.py`, `bybit.py`, `okx.py`, `kraken.py` — added `from __future__ import annotations`, `from typing import Any`, and full type hints on `_import_data` and `import_data` signatures (#21)
- `dccd/histo_dl/exchange.py` — `_get_last_date` now reads `.csv` and `.parquet` files in addition to `.xlsx` instead of falling back to 2012-01-01 (#12)
- `dccd/histo_dl/exchange.py` — completed numpydoc docstrings for `_get_last_date`, `_set_by_period`, `_name_file`, `_excel_format`, `_sort_data`, `set_hierarchy` (#12)
- `dccd/tools/io.py` — documented `driver`, `username`, `password`, `host`, `port` parameters of `save_as_sql` (#12)
- `dccd/continuous_dl/exchange.py` — documented `time_step=None` tick-by-tick behaviour in `ContinuousDownloader` (#12)
- `dccd/continuous_dl/bitfinex.py` — resolved all inline TODOs, added full type annotations, removed dead `__main__` block (#12)
- `dccd/continuous_dl/bitmex.py` — resolved all inline TODOs, added full type annotations, fixed undefined `pair` variable in `get_data_bitmex`, removed dead `__main__` block (#12)
- `pyproject.toml` — removed `bitfinex` and `bitmex` from mypy `ignore_errors` override; lifted `continuous_dl/*` and `tools/websocket.py` from coverage omit (#12)

### Removed

- `ContinuousDownloader._parser_debug()` — dead method, never called; `dccd/tools/__init__.py` commented-out imports removed (#22)

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
