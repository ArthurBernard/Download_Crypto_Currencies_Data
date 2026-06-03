"""One-shot migration of legacy (v2) Parquet files to the canonical v3 schema.

Two independent transformations, both idempotent:

1. **Timestamp scale** — v2 stored ``TS`` in seconds; v3 uses nanoseconds.
2. **Column schema** — v2 OHLC used ``quoteVolume``/``weightedAverage``; v3 uses
   ``quote_volume`` (+ a ``trades`` count). ``weightedAverage`` is dropped.

A file is rewritten if *either* its timestamp scale *or* its columns differ
from canonical. Files already in v3 form are left untouched.
"""

from __future__ import annotations

import logging
import pathlib
from typing import Any

from dccd.domain.types import DataType
from dccd.storage.parquet import canonicalize

__all__ = ["migrate_parquet_to_ns", "needs_migration"]

logger = logging.getLogger(__name__)

_NS_THRESHOLD = 1_000_000_000_000_000_000  # ~2001 in ns; any real ns ts exceeds it
_DTYPE_DIRS = {
    "ohlc": DataType.OHLC,
    "trades": DataType.TRADES,
    "orderbook": DataType.ORDERBOOK,
}


def _infer_data_type(file_path: pathlib.Path) -> DataType | None:
    """Infer the DataType from the storage layout (.../{ohlc,trades,orderbook}/...)."""
    for part in file_path.parts:
        if part in _DTYPE_DIRS:
            return _DTYPE_DIRS[part]
    return None


def _needs_ts_rescale(df: Any) -> bool:
    if "TS" not in df.columns or len(df) == 0:
        return False
    return int(df["TS"].max()) < _NS_THRESHOLD


def _needs_schema_change(df: Any, data_type: DataType) -> bool:
    from dccd.storage.parquet import _SCHEMAS

    return list(df.columns) != list(_SCHEMAS[data_type].keys())


def needs_migration(file_path: pathlib.Path) -> bool:
    """Return True if the file is not already in canonical v3 form."""
    try:
        import polars as pl

        df = pl.read_parquet(file_path)
        if len(df) == 0:
            return False
        data_type = _infer_data_type(file_path)
        if data_type is None:
            return _needs_ts_rescale(df)
        return _needs_ts_rescale(df) or _needs_schema_change(df, data_type)
    except Exception:
        return False


def migrate_parquet_to_ns(
    data_path: str | pathlib.Path,
    *,
    dry_run: bool = False,
) -> list[dict[str, Any]]:
    """Migrate all Parquet files under *data_path* to the canonical v3 schema.

    Parameters
    ----------
    data_path : str or Path
        Root data directory.
    dry_run : bool
        If True, only report what would change without modifying any file.

    Returns
    -------
    list of dict
        Per-file report with ``path``, ``rows``, ``from_schema``, ``to_schema``,
        ``rescaled`` (TS s→ns applied) and ``migrated`` keys.
    """
    import polars as pl

    root = pathlib.Path(data_path)
    report: list[dict[str, Any]] = []

    for f in sorted(root.rglob("*.parquet")):
        try:
            df = pl.read_parquet(f)
        except Exception as exc:
            logger.error("Failed to read %s: %s", f, exc)
            report.append({"path": str(f), "rows": -1, "migrated": False, "error": str(exc)})
            continue

        n = len(df)
        data_type = _infer_data_type(f)
        from_schema = list(df.columns)
        rescale = _needs_ts_rescale(df)
        schema_change = data_type is not None and _needs_schema_change(df, data_type)

        if n == 0 or (not rescale and not schema_change):
            report.append({
                "path": str(f), "rows": n, "from_schema": from_schema,
                "to_schema": from_schema, "rescaled": False, "migrated": False,
            })
            continue

        new = df
        if rescale:
            new = new.with_columns((pl.col("TS") * 1_000_000_000).alias("TS"))
        if schema_change and data_type is not None:
            new = canonicalize(new, data_type)
        to_schema = list(new.columns)

        if not dry_run:
            new.write_parquet(f, compression="snappy")
            logger.info("Migrated %s (%d rows): %s → %s", f, n, from_schema, to_schema)
        else:
            logger.info("[dry-run] Would migrate %s (%d rows): %s → %s",
                        f, n, from_schema, to_schema)

        report.append({
            "path": str(f), "rows": n, "from_schema": from_schema,
            "to_schema": to_schema, "rescaled": rescale, "migrated": not dry_run,
        })

    return report
