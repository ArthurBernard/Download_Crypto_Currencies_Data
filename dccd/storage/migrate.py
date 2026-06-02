"""One-shot migration: seconds-timestamps → nanoseconds in existing Parquet files."""

from __future__ import annotations

import logging
import pathlib
from typing import Any

__all__ = ["migrate_parquet_to_ns", "needs_migration"]

logger = logging.getLogger(__name__)

_NS_THRESHOLD = 1_000_000_000_000_000_000  # 2001 in ns; 2001 in s = far future


def needs_migration(file_path: pathlib.Path) -> bool:
    """Return True if the file has second-scale TS values (pre-v3 format)."""
    try:
        import polars as pl
        df = pl.read_parquet(file_path, columns=["TS"])
        if len(df) == 0:
            return False
        max_ts = int(df["TS"].max())
        return max_ts < _NS_THRESHOLD
    except Exception:
        return False


def migrate_parquet_to_ns(
    data_path: str | pathlib.Path,
    *,
    dry_run: bool = False,
) -> list[dict[str, Any]]:
    """Migrate all Parquet files under *data_path* from seconds to nanoseconds.

    Parameters
    ----------
    data_path : str or Path
        Root data directory.
    dry_run : bool
        If True, only report which files would be changed without modifying them.

    Returns
    -------
    list of dict
        Report of processed files with ``path``, ``rows``, ``migrated`` keys.
    """
    import polars as pl

    root = pathlib.Path(data_path)
    report: list[dict[str, Any]] = []

    for f in sorted(root.rglob("*.parquet")):
        if not needs_migration(f):
            report.append({"path": str(f), "rows": 0, "migrated": False})
            continue

        try:
            df = pl.read_parquet(f)
            n = len(df)
            if not dry_run:
                df = df.with_columns((pl.col("TS") * 1_000_000_000).alias("TS"))
                df.write_parquet(f)
                logger.info("Migrated %s (%d rows)", f, n)
            else:
                logger.info("[dry-run] Would migrate %s (%d rows)", f, n)
            report.append({"path": str(f), "rows": n, "migrated": not dry_run})
        except Exception as exc:
            logger.error("Failed to migrate %s: %s", f, exc)
            report.append({"path": str(f), "rows": -1, "migrated": False, "error": str(exc)})

    return report
