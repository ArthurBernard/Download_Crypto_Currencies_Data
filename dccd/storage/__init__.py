"""Storage layer — Parquet store, SQLite runs store, remote sync, migration."""

# Compat re-export for legacy code (histo_dl, daemon) — removed in P8
from dccd._storage_v2 import DataStore
from dccd.storage.parquet import ParquetStore
from dccd.storage.runs_sqlite import RunsStore

__all__ = ["ParquetStore", "RunsStore", "DataStore"]
