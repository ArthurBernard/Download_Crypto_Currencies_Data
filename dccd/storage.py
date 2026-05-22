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
from typing import TYPE_CHECKING

import pandas as pd

from dccd.tools.date_time import TS_to_date, span_label

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
        groups: dict[str, pd.DataFrame] = {}
        for ts, row in zip(df['TS'], df.itertuples(index=False)):
            label = TS_to_date(int(ts), form=fmt, tz='UTC')
            groups.setdefault(label, []).append(row._asdict())  # type: ignore[attr-defined]

        for label, rows in groups.items():
            file_path = self.directory / f'{label}.parquet'
            new = pd.DataFrame(rows)
            if file_path.exists():
                try:
                    existing = pd.read_parquet(file_path)
                    merged = (
                        pd.concat([existing, new], ignore_index=True)
                        .drop_duplicates(subset='TS', keep='last')
                        .sort_values('TS')
                        .reset_index(drop=True)
                    )
                except Exception:
                    logger.warning('Corrupted file %s — overwriting.', file_path)
                    merged = new.drop_duplicates(subset='TS', keep='last').sort_values('TS').reset_index(drop=True)
            else:
                merged = new.drop_duplicates(subset='TS', keep='last').sort_values('TS').reset_index(drop=True)
            merged.to_parquet(file_path, index=False)

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

    def missing_intervals(self, start: int, end: int) -> list[tuple[int, int]]:
        """Return intervals within ``[start, end]`` that need to be downloaded.

        In 3.1 this is a stub that always returns ``[(start, end)]`` (current
        behaviour — resume from last saved point, no gap detection).  Full
        gap-filling logic is implemented in section 3.2.

        Parameters
        ----------
        start : int
            Desired start timestamp (Unix seconds).
        end : int
            Desired end timestamp (Unix seconds).

        Returns
        -------
        list of (int, int)
            List of ``(start, end)`` pairs to download.

        """
        last = self.last_timestamp()
        effective_start = max(start, last) if last is not None else start
        if effective_start >= end:
            return []
        return [(effective_start, end)]
