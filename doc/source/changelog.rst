=========
Changelog
=========

Full history of all notable changes, organised by release.

The project follows `Semantic Versioning <https://semver.org/>`_ and the
`Keep a Changelog <https://keepachangelog.com/>`_ format.

.. grid:: 1
   :gutter: 2

   .. grid-item-card:: View on GitHub
      :link: https://github.com/ArthurBernard/Download_Crypto_Currencies_Data/blob/master/CHANGELOG.md
      :link-type: url

      Browse the full ``CHANGELOG.md`` with syntax highlighting directly on
      GitHub, including the ``[Unreleased]`` section for upcoming changes.

----

Recent releases
---------------

.. rubric:: [Unreleased]

- Sphinx documentation refonte: structured sidebar, exchange-specific pages,
  hero banner, installation guide, CLI reference, configuration reference,
  candlestick logo (#59)

.. rubric:: [2.3.2] — 2026-05-25

**Added**

- ``dccd status --json``: emit raw metrics as JSON for Grafana / jq (#53)
- ``HistoJob.max_retries`` and ``HistoJob.retry_delay``: per-job retry config with
  exponential back-off (#53)
- ``resolve_config_path()`` and XDG fallback (``~/.config/dccd/config.yml``) (#49)
- ``dccd inventory``: scan ``data_path`` and print a table of all stored data with
  date range, row count, and gap count (#50)
- ``dccd remove``: remove a pair from a histo_job and re-validate before writing (#50)

**Changed**

- Replace pandas with Polars throughout storage, histo_dl, backfill, process_data,
  and stream_manager; ``get_data()`` now returns ``pl.DataFrame`` by default (#52)
- Backfill progress bar shows current window date instead of raw count (#48)

**Fixed**

- ``_sort_data`` no longer crashes with ``ColumnNotFoundError: "date"`` on empty API
  responses after the Polars migration (#54)

.. rubric:: [2.3.1] — 2026-05-24

**Fixed**

- ``DataStore.missing_intervals`` now detects gaps before the first saved row (#46)
- Coinbase: raise ``RuntimeError`` on HTTP 200 with a JSON dict instead of silently
  crashing (#45)
- ``_sort_data`` no longer raises ``KeyError: 'TS'`` on empty data (#45)
- OKX: switch from ``/market/candles`` to ``/market/history-candles`` (#45)

.. rubric:: [2.3.0] — 2026-05-22

Major release introducing the autonomous daemon, Polars migration, and Pydantic v2
models.  See the full
`CHANGELOG <https://github.com/ArthurBernard/Download_Crypto_Currencies_Data/blob/master/CHANGELOG.md>`_
for details.
