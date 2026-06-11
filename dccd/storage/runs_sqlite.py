"""SQLite-backed runs store for job execution history."""

from __future__ import annotations

import json
import logging
import pathlib
import sqlite3
from contextlib import contextmanager
from typing import Any, Generator

__all__ = ["RunsStore"]

logger = logging.getLogger(__name__)

_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS runs (
    run_id      TEXT PRIMARY KEY,
    spec_id     TEXT NOT NULL,
    operation   TEXT NOT NULL,
    exchange    TEXT NOT NULL,
    symbol      TEXT NOT NULL,
    data_type   TEXT NOT NULL,
    state       TEXT NOT NULL,
    started_at  INTEGER,
    ended_at    INTEGER,
    rows_written INTEGER DEFAULT 0,
    error       TEXT,
    progress    TEXT,
    log_tail    TEXT DEFAULT '[]',
    created_at  INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_runs_spec ON runs(spec_id);
CREATE INDEX IF NOT EXISTS idx_runs_state ON runs(state);
CREATE INDEX IF NOT EXISTS idx_runs_created ON runs(created_at);
"""


class RunsStore:
    """Append-only SQLite store for JobRun records.

    Parameters
    ----------
    db_path : str or Path
        Path to the SQLite database file. Created if absent.

    Examples
    --------
    >>> import tempfile, os
    >>> with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
    ...     path = f.name
    >>> store = RunsStore(path)
    >>> store.create_run('r1', 'backfill:binance:BTC/USDT:ohlc', 'backfill', 'binance', 'BTC/USDT', 'ohlc')
    >>> os.unlink(path)
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

    def create_run(
        self,
        run_id: str,
        spec_id: str,
        operation: str,
        exchange: str,
        symbol: str,
        data_type: str,
        started_at: int | None = None,
    ) -> None:
        """Insert a new run row in the ``running`` state."""
        import time
        now = int(time.time() * 1_000_000_000)
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO runs (run_id, spec_id, operation, exchange, symbol,
                   data_type, state, started_at, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, 'running', ?, ?)""",
                (run_id, spec_id, operation, exchange, symbol, data_type, started_at, now),
            )

    def finish_run(
        self,
        run_id: str,
        state: str,
        ended_at: int | None = None,
        rows_written: int = 0,
        error: str | None = None,
    ) -> None:
        """Mark a run finished with its final state, row count and optional error."""
        import time
        if ended_at is None:
            ended_at = int(time.time() * 1_000_000_000)
        with self._conn() as conn:
            conn.execute(
                """UPDATE runs SET state=?, ended_at=?, rows_written=?, error=?
                   WHERE run_id=?""",
                (state, ended_at, rows_written, error, run_id),
            )

    def update_progress(self, run_id: str, progress: dict[str, Any]) -> None:
        """Persist the latest progress dict for *run_id* (polled by the UI)."""
        with self._conn() as conn:
            conn.execute(
                "UPDATE runs SET progress=? WHERE run_id=?",
                (json.dumps(progress), run_id),
            )

    def append_log(self, run_id: str, msg: str, max_lines: int = 100) -> None:
        """Append a log line to the run's bounded ``log_tail``."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT log_tail FROM runs WHERE run_id=?", (run_id,)
            ).fetchone()
            if row is None:
                return
            lines = json.loads(row["log_tail"])
            lines.append(msg)
            if len(lines) > max_lines:
                lines = lines[-max_lines:]
            conn.execute(
                "UPDATE runs SET log_tail=? WHERE run_id=?",
                (json.dumps(lines), run_id),
            )

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        """Return one run as a dict, or None if unknown."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM runs WHERE run_id=?", (run_id,)
            ).fetchone()
            return dict(row) if row else None

    def list_runs(
        self,
        spec_id: str | None = None,
        state: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """List recent runs, most recent first, optionally filtered."""
        conditions = []
        params: list[Any] = []
        if spec_id:
            conditions.append("spec_id=?")
            params.append(spec_id)
        if state:
            conditions.append("state=?")
            params.append(state)
        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        params.append(limit)
        with self._conn() as conn:
            rows = conn.execute(
                f"SELECT * FROM runs {where} ORDER BY created_at DESC LIMIT ?",
                params,
            ).fetchall()
            return [dict(r) for r in rows]

    def active_runs(self) -> list[dict[str, Any]]:
        """Runs currently ``running`` or ``reconnecting``."""
        return self.list_runs(state="running") + self.list_runs(state="reconnecting")

    def mark_stale_running(self) -> int:
        """Transition all ``running`` rows to ``stale`` at daemon boot.

        Runs left in state ``running`` after a daemon crash or SIGKILL pollute
        :meth:`active_runs` and the Dashboard forever.  Calling this once during
        daemon startup corrects the DB without deleting any history rows.

        Parameters
        ----------
        None

        Returns
        -------
        int
            Number of rows updated (0 when the store is clean).

        Notes
        -----
        The ``ended_at`` timestamp is set to *now* (nanoseconds UTC) and
        ``error`` is set to ``'orphaned by daemon restart'`` so the run history
        clearly attributes the state change to a restart rather than a normal
        completion or a user-visible error.

        This method must only be called from the daemon boot path (FastAPI
        lifespan startup).  Calling it while a daemon is live would incorrectly
        stale-out its legitimate active runs.
        """
        import time
        now = int(time.time() * 1_000_000_000)
        with self._conn() as conn:
            cursor = conn.execute(
                """UPDATE runs SET state='stale', ended_at=?, error='orphaned by daemon restart'
                   WHERE state='running'""",
                (now,),
            )
            return cursor.rowcount
