#!/usr/bin/env python3
"""One-shot migration: rewrite pandas-style parquet files to clean polars format.

Pandas wrote extra columns (``date``, ``time``, ``Date``) alongside OHLC data.
Polars cannot deserialise those columns, causing ``DataStore.save`` to log
"Corrupted file — overwriting" and re-download data that already exists.

Run once after upgrading to the polars-based dccd:

    python examples/migrate_parquet_to_polars.py --data-path /your/data/path

The script is idempotent: files that are already clean (no offending columns)
are left untouched.  A dry-run mode (``--dry-run``) lists what would change
without writing anything.
"""
from __future__ import annotations

import argparse
import pathlib
import sys

import pyarrow.parquet as pq
import polars as pl

_LEGACY_COLS = {'date', 'time', 'Date'}


def _needs_migration(path: pathlib.Path) -> bool:
    try:
        return bool(_LEGACY_COLS & set(pq.read_schema(str(path)).names))
    except Exception:
        return False


def _migrate(path: pathlib.Path, dry_run: bool) -> bool:
    """Return True if the file was (or would be) rewritten."""
    if not _needs_migration(path):
        return False

    schema_names = pq.read_schema(str(path)).names
    keep = [c for c in schema_names if c not in _LEGACY_COLS]

    if dry_run:
        print(f"  would rewrite: {path}  (drop {_LEGACY_COLS & set(schema_names)})")
        return True

    try:
        df = pl.read_parquet(path, columns=keep)
        df.write_parquet(path)
        print(f"  migrated: {path}")
        return True
    except Exception as exc:
        print(f"  ERROR on {path}: {exc}", file=sys.stderr)
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--data-path', required=True,
                        help='Root data directory (same as storage.local_path in config.yml)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Print what would change without writing')
    args = parser.parse_args()

    root = pathlib.Path(args.data_path)
    if not root.exists():
        sys.exit(f'data-path not found: {root}')

    files = sorted(root.rglob('*.parquet'))
    if not files:
        print('No parquet files found.')
        return

    print(f'Scanning {len(files)} parquet file(s) under {root} …')
    migrated = sum(_migrate(f, args.dry_run) for f in files)
    verb = 'would migrate' if args.dry_run else 'migrated'
    print(f'\nDone — {verb} {migrated}/{len(files)} file(s).')


if __name__ == '__main__':
    main()
