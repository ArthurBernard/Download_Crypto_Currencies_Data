#!/usr/bin/env python3
# coding: utf-8

"""Unified data storage for all dccd data types.

:class:`DataStore` is the single point of entry for reading and writing
crypto data regardless of exchange, data type (OHLC, trades, order book),
or collection method (REST or WebSocket).

Directory layout
----------------
::

    {data_path}/{exchange}/ohlc/{pair}/{span}/YYYY.parquet
    {data_path}/{exchange}/trades/{pair}/YYYY-MM-DD.parquet
    {data_path}/{exchange}/orderbook/{pair}/YYYY-MM-DD.parquet

- *exchange*: lowercase (``'binance'``, ``'kraken'``…)
- *pair*: ``BTC-USDT`` (slash replaced by hyphen — slash is invalid in paths)
- *span*: short label ``'1m'``, ``'1h'``, ``'1d'``… (OHLC only)
- Granularity: **annual** for OHLC, **daily** for trades/orderbook

"""

from __future__ import annotations

import logging
import pathlib
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pandas as pd

from dccd.tools.date_time import span_label

if TYPE_CHECKING:
    pass

__all__ = ['DataStore']

logger = logging.getLogger(__name__)

_DEFAULT_START_TS: int = 1325376000  # 2012-01-01 00:00:00 UTC


class DataStore:
    """Unified read/write interface for a single (exchange, pair, data_type).

    Parameters
    ----------
    data_path : str
        Root directory for all local data files (e.g. ``'/data/crypto'``).
    exchange : str
        Exchange name, lowercase (e.g. ``'binance'``).
    pair : str
        Trading pair in ``'CRYPTO/FIAT'`` format (e.g. ``'BTC/USDT'``).
        The slash is converted to a hyphen for the file-system path.
    span : int or None
        Candle interval in seconds.  Required for ``data_type='ohlc'``;
        pass ``None`` for trades and orderbook.
    data_type : {'ohlc', 'trades', 'orderbook'}
        Kind of data stored in this instance.

    Attributes
    ----------
    directory : pathlib.Path
        Absolute directory where files are stored.  Created on first access.

    """

    def __init__(
        self,
        data_path: str,
        exchange: str,
        pair: str,
        span: int | None,
        data_type: str = 'ohlc',
    ) -> None:
        if data_type not in ('ohlc', 'trades', 'orderbook'):
            raise ValueError(
                f"data_type must be 'ohlc', 'trades', or 'orderbook', got {data_type!r}"
            )
        if data_type == 'ohlc' and span is None:
            raise ValueError("span is required for data_type='ohlc'")

        self.data_path = data_path
        self.exchange = exchange.lower()
        self._pair_slug = pair.replace('/', '-')
        self.span = span
        self.data_type = data_type
        self._dir: pathlib.Path | None = None

    @property
    def directory(self) -> pathlib.Path:
        """Absolute directory for this store (created if absent)."""
        if self._dir is None:
            root = pathlib.Path(self.data_path) / self.exchange
            if self.data_type == 'ohlc':
                assert self.span is not None
                self._dir = root / 'ohlc' / self._pair_slug / span_label(self.span)
            else:
                self._dir = root / self.data_type / self._pair_slug
            self._dir.mkdir(parents=True, exist_ok=True)
        return self._dir

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def save(self, df: pd.DataFrame) -> None:
        """Write *df* into the appropriate period file(s), merging with existing data.

        OHLC data is grouped by year; trades and orderbook by calendar day.
        Rows are merged on ``'TS'`` (dedup ``keep='last'``), sorted ascending,
        and written as Parquet.

        Parameters
        ----------
        df : pd.DataFrame
            Data to persist.  Must contain a ``'TS'`` column (Unix timestamps).

        """
        if df.empty:
            return

        if 'TS' not in df.columns:
            raise ValueError("DataFrame must contain a 'TS' column")

        if self.data_type == 'ohlc':
            self._save_grouped(df, fmt='%Y')
        else:
            self._save_grouped(df, fmt='%Y-%m-%d')

    def _save_grouped(self, df: pd.DataFrame, fmt: str) -> None:
        labels = pd.to_datetime(df['TS'], unit='s', utc=True).dt.strftime(fmt)
        for label, group_df in df.groupby(labels, sort=False):
            file_path = self.directory / f'{label}.parquet'
            new = group_df.reset_index(drop=True)
            if file_path.exists():
                try:
                    existing = pd.read_parquet(file_path)
                    new = (
                        pd.concat([existing, new], ignore_index=True)
                        .drop_duplicates(subset='TS', keep='last')
                        .sort_values('TS')
                        .reset_index(drop=True)
                    )
                except Exception:
                    logger.warning('Corrupted file %s — overwriting.', file_path)
                    new = new.drop_duplicates(subset='TS', keep='last').sort_values('TS').reset_index(drop=True)
            else:
                new = new.drop_duplicates(subset='TS', keep='last').sort_values('TS').reset_index(drop=True)
            new.to_parquet(file_path, index=False)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def load(
        self,
        start: int | None = None,
        end: int | None = None,
    ) -> pd.DataFrame:
        """Load and concatenate all period files covering ``[start, end]``.

        Parameters
        ----------
        start : int or None, optional
            Inclusive lower bound (Unix timestamp).  ``None`` means no lower
            bound.
        end : int or None, optional
            Inclusive upper bound (Unix timestamp).  ``None`` means no upper
            bound.

        Returns
        -------
        pd.DataFrame
            Concatenated data, sorted by ``'TS'``, filtered to ``[start, end]``.
            Empty DataFrame if no files are found.

        """
        files = sorted(self.directory.glob('*.parquet'))
        if not files:
            return pd.DataFrame()

        pieces: list[pd.DataFrame] = []
        for f in files:
            try:
                pieces.append(pd.read_parquet(f))
            except Exception:
                logger.warning('Skipping corrupted file %s', f)

        if not pieces:
            return pd.DataFrame()

        df = pd.concat(pieces, ignore_index=True).sort_values('TS').reset_index(drop=True)

        if start is not None:
            df = df[df['TS'] >= start]
        if end is not None:
            df = df[df['TS'] <= end]

        return df.reset_index(drop=True)

    # ------------------------------------------------------------------
    # Metadata
    # ------------------------------------------------------------------

    def existing_periods(self) -> list[str]:
        """List period labels for all available files.

        Returns
        -------
        list of str
            Sorted list of year strings (``['2024', '2025']``) for OHLC, or
            date strings (``['2026-05-20', '2026-05-21']``) for trades/orderbook.

        """
        return sorted(f.stem for f in self.directory.glob('*.parquet'))

    def last_timestamp(self) -> int | None:
        """Return the last ``TS`` value in the most recent period file.

        Returns
        -------
        int or None
            Unix timestamp of the last row, or ``None`` if no data exists.

        """
        periods = self.existing_periods()
        if not periods:
            return None

        # Iterate from the most recent period backward until a readable file is found.
        for period in reversed(periods):
            file_path = self.directory / f'{period}.parquet'
            try:
                df = pd.read_parquet(file_path, columns=['TS'])
                if not df.empty:
                    return int(df['TS'].max())
            except Exception:
                logger.warning('Corrupted file %s — removing.', file_path)
                file_path.unlink(missing_ok=True)

        return None

    def is_period_complete(self, year: int) -> bool:
        """Return True if the parquet file for *year* contains all expected rows.

        Parameters
        ----------
        year : int
            Calendar year to check (e.g. ``2024``).

        Returns
        -------
        bool
            ``False`` for non-OHLC stores, missing files, or when the row
            count is below the expected number of candles for that year.

        """
        if self.data_type != 'ohlc' or self.span is None:
            return False
        file_path = self.directory / f'{year}.parquet'
        if not file_path.exists():
            return False
        df = pd.read_parquet(file_path, columns=['TS'])
        year_start = datetime(year,     1, 1, tzinfo=timezone.utc)
        year_end   = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
        expected = int((year_end - year_start).total_seconds()) // self.span
        return len(df) >= expected

    def missing_intervals(self, start: int, end: int) -> list[tuple[int, int]]:
        """Return the list of ``(start, end)`` intervals within ``[start, end]``
        that still need to be downloaded.

        For OHLC stores the method inspects existing annual parquet files:
        complete past years are skipped entirely; incomplete or absent years
        yield an interval from the last saved timestamp (``+ span``) to the
        end of that year.  The current calendar year always extends from the
        last saved row to *end*.

        For trades / orderbook stores (no ``span``) the method falls back to a
        simple resume: one interval from ``last_timestamp + span`` (or
        *start* if no data) to *end*.

        Parameters
        ----------
        start : int
            Desired start timestamp (Unix seconds).
        end : int
            Desired end timestamp (Unix seconds).

        Returns
        -------
        list of (int, int)
            Ordered list of ``(ivl_start, ivl_end)`` pairs to download.
            Empty list means all data is already present.

        """
        if self.data_type != 'ohlc' or self.span is None:
            last = self.last_timestamp()
            effective = max(start, last) if last is not None else start
            return [(effective, end)] if effective < end else []

        current_year = datetime.now(tz=timezone.utc).year
        start_year   = datetime.fromtimestamp(start, tz=timezone.utc).year
        end_year     = datetime.fromtimestamp(end,   tz=timezone.utc).year
        intervals: list[tuple[int, int]] = []

        for year in range(start_year, end_year + 1):
            year_start_ts = int(datetime(year,     1, 1, tzinfo=timezone.utc).timestamp())
            year_end_ts   = int(datetime(year + 1, 1, 1, tzinfo=timezone.utc).timestamp())

            ivl_start = max(start, year_start_ts)
            ivl_end   = min(end,   year_end_ts)
            if ivl_start >= ivl_end:
                continue

            file_path = self.directory / f'{year}.parquet'

            if file_path.exists():
                if year < current_year and self.is_period_complete(year):
                    continue  # full year already on disk — skip
                df = pd.read_parquet(file_path, columns=['TS'])
                if not df.empty:
                    file_min = int(df['TS'].min())
                    file_max = int(df['TS'].max())
                    # Gap before the first saved row (e.g. backfill requested
                    # from an earlier date than the file's first candle)
                    if ivl_start < file_min:
                        intervals.append((ivl_start, file_min))
                    # Trailing gap after the last saved row
                    trailing = file_max + self.span
                    if trailing < ivl_end:
                        intervals.append((trailing, ivl_end))
                    continue
                # file exists but is empty: fall through to full-interval append

            if ivl_start < ivl_end:
                intervals.append((ivl_start, ivl_end))

        return intervals
