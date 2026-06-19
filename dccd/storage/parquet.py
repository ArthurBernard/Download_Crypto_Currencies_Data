"""Parquet-based storage for OHLC, trades, and order book data.

Builds on the existing DataStore logic but uses nanosecond timestamps
and stores provenance in Parquet metadata.
"""

from __future__ import annotations

import logging
import pathlib
import threading
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

_SCHEMAS: dict[DataType, dict[str, Any]] = {
    DataType.OHLC: _OHLC_SCHEMA,
    DataType.TRADES: _TRADES_SCHEMA,
    DataType.ORDERBOOK: _BOOK_SCHEMA,
}

# Legacy (v2) → canonical (v3) column renames. ``weightedAverage`` is dropped:
# v3 has no equivalent (it carries a trade-count instead, unrecoverable here).
_LEGACY_RENAME = {"quoteVolume": "quote_volume"}
_LEGACY_DROP = ("weightedAverage",)


def canonicalize(df: pl.DataFrame, data_type: DataType) -> pl.DataFrame:
    """Coerce a (possibly legacy v2) frame to the canonical v3 schema.

    Renames legacy columns, drops unsupported ones, fills missing canonical
    columns with nulls, and selects them in canonical order. Idempotent on
    frames already in v3 schema. This is the single normalisation point shared
    by reads, merges and migration, so legacy data is never lost on ``concat``.
    """
    schema = _SCHEMAS[data_type]
    if df.is_empty() and not df.columns:
        return df
    rename = {k: v for k, v in _LEGACY_RENAME.items() if k in df.columns and v not in df.columns}
    if rename:
        df = df.rename(rename)
    drop = [c for c in _LEGACY_DROP if c in df.columns]
    if drop:
        df = df.drop(drop)
    missing = [
        pl.lit(None, dtype=dtype).alias(name)
        for name, dtype in schema.items()
        if name not in df.columns
    ]
    if missing:
        df = df.with_columns(missing)
    return df.select(list(schema.keys()))


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
        # save() is a read-modify-write per period file and runs in worker
        # threads (operations flush via asyncio.to_thread). Concurrent saves to
        # the *same* file (e.g. "run all jobs", or a scheduled job overlapping a
        # manual one) would otherwise interleave and corrupt the Parquet. One
        # lock per file path serialises those while leaving different files
        # (datasets/periods) fully parallel.
        self._file_locks: dict[str, threading.Lock] = {}
        self._file_locks_guard = threading.Lock()
        # Per-file metadata cache: path str → (mtime_ns, size, rows, min_ts, max_ts).
        # Keyed by (mtime_ns, size) so any write (atomic rename → new inode + mtime)
        # automatically invalidates. The daemon holds one ParquetStore instance
        # for its lifetime; CLI processes are short-lived so stale cache is fine.
        self._stats_cache: dict[str, tuple[int, int, int, int | None, int | None]] = {}

    @property
    def root(self) -> pathlib.Path:
        """Root directory of the local store."""
        return self._root

    def _lock_for(self, file_path: pathlib.Path) -> threading.Lock:
        key = str(file_path)
        with self._file_locks_guard:
            lock = self._file_locks.get(key)
            if lock is None:
                lock = threading.Lock()
                self._file_locks[key] = lock
            return lock

    def _file_stats(
        self, f: pathlib.Path
    ) -> tuple[int, int | None, int | None]:
        """Return ``(rows, min_ts, max_ts)`` for a parquet file.

        Uses pyarrow footer metadata and row-group TS statistics to avoid
        materialising the TS column.  A per-file mtime/size cache means warm
        calls cost only one ``stat()`` syscall.  Falls back to
        ``pl.read_parquet`` when a row group has no TS statistics (legacy
        writer that didn't record min/max).

        Parameters
        ----------
        f:
            Path to an existing ``.parquet`` file.

        Returns
        -------
        tuple[int, int | None, int | None]
            ``(rows, min_ts, max_ts)`` where *min_ts* / *max_ts* are the
            nanosecond TS bounds across all row groups, or ``None`` when the
            file is empty.
        """
        try:
            st = f.stat()
        except OSError:
            return 0, None, None

        key = str(f)
        cached = self._stats_cache.get(key)
        if cached is not None:
            c_mtime, c_size, c_rows, c_min, c_max = cached
            if c_mtime == st.st_mtime_ns and c_size == st.st_size:
                return c_rows, c_min, c_max

        # Cache miss — read from parquet footer (no column materialisation).
        try:
            import pyarrow.parquet as pq  # lazy import, matching existing style

            meta = pq.ParquetFile(f).metadata
            total_rows: int = meta.num_rows
            if total_rows == 0:
                self._stats_cache[key] = (st.st_mtime_ns, st.st_size, 0, None, None)
                return 0, None, None

            # Find the TS column index (name may differ by file; scan once).
            ts_col_idx: int | None = None
            for col_idx in range(meta.row_group(0).num_columns):
                if meta.row_group(0).column(col_idx).path_in_schema == "TS":
                    ts_col_idx = col_idx
                    break

            if ts_col_idx is None:
                # TS column not found — read full file as fallback.
                raise ValueError("TS column not found in schema")

            min_ts: int | None = None
            max_ts: int | None = None
            needs_fallback = False
            for rg_idx in range(meta.num_row_groups):
                rg = meta.row_group(rg_idx)
                col_meta = rg.column(ts_col_idx)
                stats = col_meta.statistics
                if stats is None or not stats.has_min_max:
                    needs_fallback = True
                    break
                rg_min = int(stats.min)
                rg_max = int(stats.max)
                if min_ts is None or rg_min < min_ts:
                    min_ts = rg_min
                if max_ts is None or rg_max > max_ts:
                    max_ts = rg_max

            if needs_fallback:
                # Legacy writer: fall back to reading the TS column.
                df = pl.read_parquet(f, columns=["TS"])
                n = len(df)
                if n == 0:
                    self._stats_cache[key] = (st.st_mtime_ns, st.st_size, 0, None, None)
                    return 0, None, None
                min_ts = int(df["TS"].min())  # type: ignore[arg-type]
                max_ts = int(df["TS"].max())  # type: ignore[arg-type]
                total_rows = n

        except Exception:
            # Any error (corrupt file, missing dep) → try reading the column.
            try:
                df = pl.read_parquet(f, columns=["TS"])
                n = len(df)
                if n == 0:
                    self._stats_cache[key] = (st.st_mtime_ns, st.st_size, 0, None, None)
                    return 0, None, None
                min_ts = int(df["TS"].min())  # type: ignore[arg-type]
                max_ts = int(df["TS"].max())  # type: ignore[arg-type]
                total_rows = n
            except Exception:
                return 0, None, None

        self._stats_cache[key] = (st.st_mtime_ns, st.st_size, total_rows, min_ts, max_ts)
        return total_rows, min_ts, max_ts

    def directory(self, ds: DatasetId) -> pathlib.Path:
        """Return the directory for *ds*, creating it if needed."""
        pair_slug = ds.pair_slug()
        root = self._root / ds.exchange
        if ds.data_type == DataType.OHLC:
            if ds.span is None:
                raise ValueError(
                    f"DatasetId {ds} has data_type=OHLC but span is None. "
                    "Set span when constructing the DatasetId."
                )
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

        # Reject bars with an invalid timestamp (null or <= 0). TS is ns UTC
        # int64; 0 is the Unix epoch (1970) — always corrupt for crypto market
        # data (real history starts ~2009). One such row poisons gap detection
        # (inventory min_ts → 1970, expected_rows balloons). Drop, don't raise,
        # so one bad bar can't abort a good page. Seen in prod: a Kraken OHLC bar
        # with a null time parsed to 0 (audit 2026-06-19).
        n_before = len(df)
        df = df.filter(pl.col("TS").is_not_null() & (pl.col("TS") > 0))
        dropped = n_before - len(df)
        if dropped:
            logger.warning("save(%s): dropped %d row(s) with invalid TS<=0", ds, dropped)
        if len(df) == 0:
            return 0

        fmt = self._period_fmt(ds)
        df_with_period = df.with_columns(
            pl.from_epoch("TS", time_unit="ns").dt.strftime(fmt).alias("_period")
        )

        total_written = 0
        for period in df_with_period["_period"].unique().sort().to_list():
            incoming = df_with_period.filter(pl.col("_period") == period).drop("_period")
            # Count incoming rows *before* merge — this is what the caller
            # should see as "rows written" (not the post-dedup file size).
            total_written += len(incoming)
            file_path = self._file_path(ds, period)
            # Serialise the read-modify-write of this file against concurrent
            # saves so the Parquet can't be corrupted or lose an update.
            with self._lock_for(file_path):
                merged = self._merge(file_path, incoming, ds)
                self._write_parquet(file_path, merged, provenance)

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
                df = canonicalize(pl.read_parquet(f), ds.data_type)
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
            _, _, max_ts = self._file_stats(f)
            if max_ts is not None:
                return max_ts
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
                rows, file_min, file_max = self._file_stats(file_path)
                if rows > 0 and file_min is not None and file_max is not None:
                    if ivl_start < file_min:
                        intervals.append((ivl_start, file_min))
                    trailing = file_max + span_ns
                    if trailing < ivl_end:
                        intervals.append((trailing, ivl_end))
                    continue

            intervals.append((ivl_start, ivl_end))

        return intervals

    def inventory(self) -> list[dict[str, Any]]:
        """Return list of dataset info dicts for all stored data.

        Each entry includes ``min_ts`` / ``max_ts`` (nanoseconds UTC) and
        ``rows`` so the UI can display the actual data time range.
        """
        result = []
        for exchange_dir in sorted(self._root.iterdir()):
            if not exchange_dir.is_dir() or exchange_dir.name.startswith("."):
                continue
            exchange = exchange_dir.name
            for dtype_dir in sorted(exchange_dir.iterdir()):
                if not dtype_dir.is_dir():
                    continue
                dtype = dtype_dir.name
                if dtype not in ("ohlc", "trades", "orderbook"):
                    continue
                for pair_dir in sorted(dtype_dir.iterdir()):
                    if not pair_dir.is_dir():
                        continue
                    pair = pair_dir.name
                    if dtype == "ohlc":
                        for span_dir in sorted(pair_dir.iterdir()):
                            if not span_dir.is_dir():
                                continue
                            files = sorted(span_dir.glob("*.parquet"))
                            if files:
                                from dccd.domain.timeutils import str_to_span
                                span_s = str_to_span(span_dir.name)
                                if span_s is None:
                                    # Fallback: parse "3600s" format
                                    try:
                                        span_s = int(span_dir.name.rstrip("s"))
                                    except ValueError:
                                        span_s = None
                                min_ts, max_ts, rows = self._ts_range(files)
                                # Gap detection (OHLC only) is free: we already
                                # have rows + min/max + span, so the number of
                                # missing bars is pure arithmetic, no extra read.
                                expected = missing = None
                                if span_s and min_ts is not None and max_ts is not None:
                                    from dccd.domain.timeutils import NS
                                    expected = (max_ts - min_ts) // (span_s * NS) + 1
                                    missing = max(0, expected - rows)
                                result.append({
                                    "exchange": exchange,
                                    "pair": pair,
                                    "data_type": dtype,
                                    "span": span_s,
                                    "files": len(files),
                                    "rows": rows,
                                    "min_ts": min_ts,
                                    "max_ts": max_ts,
                                    "bytes": self._dir_bytes(files),
                                    "expected_rows": expected,
                                    "missing_rows": missing,
                                })
                    else:
                        files = sorted(pair_dir.glob("*.parquet"))
                        if files:
                            min_ts, max_ts, rows = self._ts_range(files)
                            result.append({
                                "exchange": exchange,
                                "pair": pair,
                                "data_type": dtype,
                                "span": None,
                                "files": len(files),
                                "rows": rows,
                                "min_ts": min_ts,
                                "max_ts": max_ts,
                                "bytes": self._dir_bytes(files),
                                "expected_rows": None,
                                "missing_rows": None,
                            })
        return result

    @staticmethod
    def _dir_bytes(files: list[pathlib.Path]) -> int:
        """Total size on disk (bytes) of a list of parquet files."""
        total = 0
        for f in files:
            try:
                total += f.stat().st_size
            except OSError:
                pass
        return total

    def _ts_range(
        self, files: list[pathlib.Path]
    ) -> tuple[int | None, int | None, int]:
        """Return (min_ts_ns, max_ts_ns, total_rows) across a list of parquet files."""
        min_ts: int | None = None
        max_ts: int | None = None
        total_rows = 0
        for f in files:
            n, fmin, fmax = self._file_stats(f)
            if n == 0:
                continue
            total_rows += n
            if fmin is not None and (min_ts is None or fmin < min_ts):
                min_ts = fmin
            if fmax is not None and (max_ts is None or fmax > max_ts):
                max_ts = fmax
        return min_ts, max_ts, total_rows

    def _is_year_complete(self, ds: DatasetId, year: int) -> bool:
        if ds.span is None:
            return False
        file_path = self._file_path(ds, str(year))
        if not file_path.exists():
            return False
        try:
            rows, _, _ = self._file_stats(file_path)
            year_start = datetime(year, 1, 1, tzinfo=timezone.utc)
            year_end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
            expected = int((year_end - year_start).total_seconds()) // ds.span
            return rows >= expected
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

    def _dedup_subset(self, ds: DatasetId, df: pl.DataFrame) -> list[str]:
        """Natural dedup key for *ds*. ``TS`` alone is unique only for OHLC.

        Trades collide on TS (exchanges timestamp at ms → many share a ns), so
        deduping on TS would drop distinct trades; we key on the trade id when
        present, else a composite. Order-book rows share one TS across every
        price level, so they key on (TS, side, price).
        """
        if ds.data_type == DataType.OHLC:
            return ["TS"]
        if ds.data_type == DataType.TRADES:
            if "tid" in df.columns and df["tid"].null_count() == 0:
                return ["tid"]
            return ["TS", "price", "amount", "side"]
        return ["TS", "side", "price"]  # order book level

    def _merge(self, file_path: pathlib.Path, new: pl.DataFrame, ds: DatasetId) -> pl.DataFrame:
        """Merge new data with existing file, deduplicating on the natural key.

        The existing file is **canonicalised** before the concat so that legacy
        (v2) files — whose columns differ (``quoteVolume``/``weightedAverage``)
        — are aligned to the v3 schema instead of raising a schema error. We
        never silently overwrite on a read error: an unreadable file is a fault
        worth surfacing, not a reason to drop its rows.
        """
        if not file_path.exists():
            return new.unique(subset=self._dedup_subset(ds, new), keep="last").sort("TS")
        existing = canonicalize(pl.read_parquet(file_path), ds.data_type)
        merged = pl.concat([existing, new])
        return merged.unique(subset=self._dedup_subset(ds, merged), keep="last").sort("TS")

    def _write_parquet(
        self,
        file_path: pathlib.Path,
        df: pl.DataFrame,
        provenance: Provenance | None,
    ) -> None:
        meta: dict[str, str] = {}
        if provenance is not None:
            meta["dccd.provenance"] = provenance.model_dump_json()
        # Write to a temp file then atomically rename, so a concurrent reader
        # (load/last_timestamp/inventory) never sees a half-written file — it
        # observes either the old complete file or the new one.
        import os

        tmp = file_path.with_suffix(file_path.suffix + f".tmp.{os.getpid()}.{threading.get_ident()}")
        try:
            # Polars >=1.x persists key/value metadata into the Parquet footer.
            df.write_parquet(tmp, compression="snappy", metadata=meta or None)
            os.replace(tmp, file_path)
        finally:
            if tmp.exists():
                tmp.unlink(missing_ok=True)

    @staticmethod
    def read_provenance(file_path: str | pathlib.Path) -> Provenance | None:
        """Return the :class:`Provenance` stored in a Parquet file, if any."""
        import json

        import pyarrow.parquet as pq

        kv = pq.read_metadata(str(file_path)).metadata or {}
        raw = kv.get(b"dccd.provenance") or kv.get("dccd.provenance")
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode()
        return Provenance(**json.loads(raw))
