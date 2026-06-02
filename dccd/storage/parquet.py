"""Parquet-based storage for OHLC, trades, and order book data.

Builds on the existing DataStore logic but uses nanosecond timestamps
and stores provenance in Parquet metadata.
"""

from __future__ import annotations

import logging
import pathlib
from datetime import datetime, timezone
from typing import Any

import polars as pl

from dccd.domain.dataset import DatasetId, Provenance
from dccd.domain.timeutils import NS, ns_to_dt, span_label
from dccd.domain.types import DataType

__all__ = ["ParquetStore"]

logger = logging.getLogger(__name__)

_OHLC_SCHEMA = {
    "TS": pl.Int64,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "volume": pl.Float64,
    "quote_volume": pl.Float64,
    "trades": pl.Int64,
}

_TRADES_SCHEMA = {
    "TS": pl.Int64,
    "price": pl.Float64,
    "amount": pl.Float64,
    "side": pl.Utf8,
    "tid": pl.Utf8,
}

_BOOK_SCHEMA = {
    "TS": pl.Int64,
    "side": pl.Utf8,
    "price": pl.Float64,
    "amount": pl.Float64,
    "count": pl.Int64,
    "is_snapshot": pl.Boolean,
}


class ParquetStore:
    """Read/write interface for a single DatasetId.

    All timestamps (``TS``) are **nanoseconds UTC** (int64).

    Parameters
    ----------
    data_path : str or Path
        Root directory for all data files.

    Examples
    --------
    >>> import pathlib, tempfile
    >>> from dccd.domain.dataset import DatasetId
    >>> from dccd.domain.symbol import Symbol
    >>> from dccd.domain.types import DataType
    >>> store = ParquetStore('/tmp/data')
    """

    def __init__(self, data_path: str | pathlib.Path) -> None:
        self._root = pathlib.Path(data_path)

    def directory(self, ds: DatasetId) -> pathlib.Path:
        """Return the directory for *ds*, creating it if needed."""
        pair_slug = ds.pair_slug()
        root = self._root / ds.exchange
        if ds.data_type == DataType.OHLC:
            assert ds.span is not None
            d = root / "ohlc" / pair_slug / span_label(ds.span)
        else:
            d = root / ds.data_type.value / pair_slug
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _period_fmt(self, ds: DatasetId) -> str:
        return "%Y" if ds.data_type == DataType.OHLC else "%Y-%m-%d"

    def _file_path(self, ds: DatasetId, period: str) -> pathlib.Path:
        return self.directory(ds) / f"{period}.parquet"

    def save(
        self,
        ds: DatasetId,
        records: list[Any],
        provenance: Provenance | None = None,
    ) -> int:
        """Write *records* to Parquet, merging with existing data.

        Parameters
        ----------
        ds : DatasetId
        records : list
            OHLCBar, Trade, or OrderBookSnapshot objects.
        provenance : Provenance or None

        Returns
        -------
        int
            Number of rows written.
        """
        if not records:
            return 0

        df = self._to_dataframe(ds, records)
        if len(df) == 0:
            return 0

        fmt = self._period_fmt(ds)
        df_with_period = df.with_columns(
            pl.from_epoch("TS", time_unit="ns").dt.strftime(fmt).alias("_period")
        )

        total_written = 0
        for period in df_with_period["_period"].unique().sort().to_list():
            group = df_with_period.filter(pl.col("_period") == period).drop("_period")
            file_path = self._file_path(ds, period)
            group = self._merge(file_path, group, ds)
            self._write_parquet(file_path, group, provenance)
            total_written += len(group)

        return total_written

    def load(
        self,
        ds: DatasetId,
        start_ns: int | None = None,
        end_ns: int | None = None,
    ) -> pl.DataFrame:
        """Load data for *ds* in the given nanosecond range."""
        directory = self.directory(ds)
        files = sorted(directory.glob("*.parquet"))
        if not files:
            return pl.DataFrame()

        pieces = []
        for f in files:
            try:
                df = pl.read_parquet(f)
                if start_ns is not None:
                    df = df.filter(pl.col("TS") >= start_ns)
                if end_ns is not None:
                    df = df.filter(pl.col("TS") <= end_ns)
                if len(df) > 0:
                    pieces.append(df)
            except Exception:
                logger.warning("Corrupted parquet file %s — skipping", f)

        if not pieces:
            return pl.DataFrame()
        return pl.concat(pieces).sort("TS")

    def last_timestamp(self, ds: DatasetId) -> int | None:
        """Return last TS in ns, or None if no data."""
        directory = self.directory(ds)
        files = sorted(directory.glob("*.parquet"), reverse=True)
        for f in files:
            try:
                df = pl.read_parquet(f, columns=["TS"])
                if len(df) > 0:
                    return int(df["TS"].max())
            except Exception:
                pass
        return None

    def missing_intervals(
        self, ds: DatasetId, start_ns: int, end_ns: int
    ) -> list[tuple[int, int]]:
        """Return gaps as (start_ns, end_ns) pairs within [start_ns, end_ns]."""
        if ds.data_type != DataType.OHLC or ds.span is None:
            last = self.last_timestamp(ds)
            effective = max(start_ns, last + 1) if last is not None else start_ns
            return [(effective, end_ns)] if effective < end_ns else []

        span_ns = ds.span * NS
        current_year = datetime.now(tz=timezone.utc).year
        start_dt = ns_to_dt(start_ns)
        end_dt = ns_to_dt(end_ns)
        intervals: list[tuple[int, int]] = []

        for year in range(start_dt.year, end_dt.year + 1):
            year_start_ns = int(datetime(year, 1, 1, tzinfo=timezone.utc).timestamp()) * NS
            year_end_ns = int(datetime(year + 1, 1, 1, tzinfo=timezone.utc).timestamp()) * NS
            ivl_start = max(start_ns, year_start_ns)
            ivl_end = min(end_ns, year_end_ns)
            if ivl_start >= ivl_end:
                continue

            file_path = self._file_path(ds, str(year))
            if file_path.exists():
                if year < current_year and self._is_year_complete(ds, year):
                    continue
                try:
                    df = pl.read_parquet(file_path, columns=["TS"])
                    if len(df) > 0:
                        file_min = int(df["TS"].min())
                        file_max = int(df["TS"].max())
                        if ivl_start < file_min:
                            intervals.append((ivl_start, file_min))
                        trailing = file_max + span_ns
                        if trailing < ivl_end:
                            intervals.append((trailing, ivl_end))
                        continue
                except Exception:
                    pass

            intervals.append((ivl_start, ivl_end))

        return intervals

    def inventory(self) -> list[dict[str, Any]]:
        """Return list of dataset info dicts for all stored data."""
        result = []
        for exchange_dir in sorted(self._root.iterdir()):
            if not exchange_dir.is_dir():
                continue
            exchange = exchange_dir.name
            for dtype_dir in sorted(exchange_dir.iterdir()):
                if not dtype_dir.is_dir():
                    continue
                dtype = dtype_dir.name
                for pair_dir in sorted(dtype_dir.iterdir()):
                    if not pair_dir.is_dir():
                        continue
                    pair = pair_dir.name
                    if dtype == "ohlc":
                        for span_dir in sorted(pair_dir.iterdir()):
                            if not span_dir.is_dir():
                                continue
                            files = list(span_dir.glob("*.parquet"))
                            if files:
                                result.append({
                                    "exchange": exchange,
                                    "pair": pair,
                                    "data_type": dtype,
                                    "span": span_dir.name,
                                    "files": len(files),
                                })
                    else:
                        files = list(pair_dir.glob("*.parquet"))
                        if files:
                            result.append({
                                "exchange": exchange,
                                "pair": pair,
                                "data_type": dtype,
                                "files": len(files),
                            })
        return result

    def _is_year_complete(self, ds: DatasetId, year: int) -> bool:
        if ds.span is None:
            return False
        file_path = self._file_path(ds, str(year))
        if not file_path.exists():
            return False
        try:
            df = pl.read_parquet(file_path, columns=["TS"])
            year_start = datetime(year, 1, 1, tzinfo=timezone.utc)
            year_end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
            expected = int((year_end - year_start).total_seconds()) // ds.span
            return len(df) >= expected
        except Exception:
            return False

    def _to_dataframe(self, ds: DatasetId, records: list[Any]) -> pl.DataFrame:
        if ds.data_type == DataType.OHLC:
            rows = [
                {
                    "TS": r.ts,
                    "open": r.open,
                    "high": r.high,
                    "low": r.low,
                    "close": r.close,
                    "volume": r.volume,
                    "quote_volume": r.quote_volume,
                    "trades": r.trades,
                }
                for r in records
            ]
            return pl.DataFrame(rows, schema=_OHLC_SCHEMA)
        elif ds.data_type == DataType.TRADES:
            rows = [
                {
                    "TS": r.ts,
                    "price": r.price,
                    "amount": r.amount,
                    "side": r.side,
                    "tid": r.tid,
                }
                for r in records
            ]
            return pl.DataFrame(rows, schema=_TRADES_SCHEMA)
        else:
            rows = []
            for snap in records:
                for lvl in snap.bids:
                    rows.append({
                        "TS": snap.ts,
                        "side": "bid",
                        "price": lvl.price,
                        "amount": lvl.amount,
                        "count": lvl.count,
                        "is_snapshot": snap.is_snapshot,
                    })
                for lvl in snap.asks:
                    rows.append({
                        "TS": snap.ts,
                        "side": "ask",
                        "price": lvl.price,
                        "amount": lvl.amount,
                        "count": lvl.count,
                        "is_snapshot": snap.is_snapshot,
                    })
            return pl.DataFrame(rows, schema=_BOOK_SCHEMA)

    def _merge(self, file_path: pathlib.Path, new: pl.DataFrame, ds: DatasetId) -> pl.DataFrame:
        """Merge new data with existing file, deduplicating on TS."""
        if not file_path.exists():
            return new.unique(subset=["TS"], keep="last").sort("TS")
        try:
            existing = pl.read_parquet(file_path)
            merged = (
                pl.concat([existing, new])
                .unique(subset=["TS"], keep="last")
                .sort("TS")
            )
            return merged
        except Exception:
            logger.warning("Could not read %s — overwriting", file_path)
            return new.unique(subset=["TS"], keep="last").sort("TS")

    def _write_parquet(
        self,
        file_path: pathlib.Path,
        df: pl.DataFrame,
        provenance: Provenance | None,
    ) -> None:
        meta: dict[str, str] = {}
        if provenance is not None:
            meta["dccd.provenance"] = provenance.model_dump_json()
        df.write_parquet(file_path, compression="snappy")
