"""SQLite-backed coverage manifest.

Records, per dataset, the extent of data we have collected — independent of the
Parquet files themselves. This is what lets local data be **dropped** (to free
disk) without forcing a re-download: ``backfill(start="last")`` falls back to the
manifest's ``max_ts`` when no local file is present, so it resumes from where
collection left off instead of from the bounded default lookback.

Lives at ``{data_path}/.dccd/coverage.db`` — the ``.dccd`` directory is never
purged, so the manifest survives a local-data drop.
"""

from __future__ import annotations

import logging
import pathlib
import sqlite3
import time
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Generator

if TYPE_CHECKING:
    from dccd.domain.dataset import DatasetId

__all__ = ["CoverageStore"]

logger = logging.getLogger(__name__)

_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS coverage (
    dataset_id  TEXT PRIMARY KEY,
    exchange    TEXT NOT NULL,
    symbol      TEXT NOT NULL,
    data_type   TEXT NOT NULL,
    span        INTEGER,
    min_ts      INTEGER,
    max_ts      INTEGER,
    rows        INTEGER DEFAULT 0,
    updated_at  INTEGER NOT NULL
);
"""


class CoverageStore:
    """Per-dataset coverage manifest (min/max ts + row count).

    Parameters
    ----------
    db_path : str or Path
        Path to the SQLite database file. Created if absent.
    """

    def __init__(self, db_path: str | pathlib.Path) -> None:
        self._path = pathlib.Path(db_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._conn() as conn:
            conn.executescript(_SCHEMA)

    @contextmanager
    def _conn(self) -> Generator[sqlite3.Connection, None, None]:
        conn = sqlite3.connect(str(self._path))
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def record(
        self,
        ds: "DatasetId",
        *,
        min_ts: int | None,
        max_ts: int | None,
        rows_added: int = 0,
    ) -> None:
        """Upsert coverage for *ds*, widening the [min_ts, max_ts] envelope.

        ``min_ts``/``max_ts`` are merged with any existing row (min of mins, max
        of maxes), so a later backfill never *narrows* the recorded extent;
        ``rows_added`` accumulates (an approximate stored-row tally for display).
        """
        key = str(ds)
        now = int(time.time() * 1_000_000_000)
        with self._conn() as conn:
            row = conn.execute(
                "SELECT min_ts, max_ts, rows FROM coverage WHERE dataset_id=?",
                (key,),
            ).fetchone()
            if row is not None:
                new_min = _merge(row["min_ts"], min_ts, min)
                new_max = _merge(row["max_ts"], max_ts, max)
                new_rows = (row["rows"] or 0) + rows_added
                conn.execute(
                    "UPDATE coverage SET min_ts=?, max_ts=?, rows=?, updated_at=? "
                    "WHERE dataset_id=?",
                    (new_min, new_max, new_rows, now, key),
                )
            else:
                conn.execute(
                    "INSERT INTO coverage (dataset_id, exchange, symbol, "
                    "data_type, span, min_ts, max_ts, rows, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (key, ds.exchange, str(ds.symbol), ds.data_type.value,
                     ds.span, min_ts, max_ts, rows_added, now),
                )

    def get_max_ts(self, ds: "DatasetId") -> int | None:
        """Return the recorded ``max_ts`` for *ds*, or ``None`` if unknown."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT max_ts FROM coverage WHERE dataset_id=?", (str(ds),)
            ).fetchone()
            return row["max_ts"] if row else None

    def list_all(self) -> list[dict[str, Any]]:
        """Return every coverage row (most recently updated first)."""
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM coverage ORDER BY updated_at DESC"
            ).fetchall()
            return [dict(r) for r in rows]


def _merge(existing: int | None, incoming: int | None, op: Any) -> int | None:
    """Combine two optional ints with *op* (min/max), ignoring ``None``."""
    vals = [v for v in (existing, incoming) if v is not None]
    return op(vals) if vals else None
