#!/usr/bin/env python3
"""Delete corrupted Kraken and OKX parquet files so the backfill re-downloads them.

Kraken bug: TS shifted by local timezone offset + candles not aligned to the
minute (start_by='datapoint' → bins start at first-trade timestamp, not 00s).

OKX bug: max_candles was set to 990 but the API returns 300 max, so each
990-minute window contained 690 forward-filled null rows (69 % of the data).

Both bugs are fixed in backfill.py. Run this script first, then:

    dccd backfill --config config.yml --exchange kraken --start "2024-01-01"
    dccd backfill --config config.yml --exchange okx    --start "2024-01-01"
"""

from __future__ import annotations

import sys
from pathlib import Path

BASE = Path("~/data/crypto").expanduser()

TARGETS = {
    "kraken": "timezone + alignment bug — all TS shifted by UTC offset, "
              "candles not aligned to the minute",
    "okx":    "max_candles=990 was wrong (API max is 300) — 69 % of rows are "
              "forward-filled nulls",
}


def _list_files(exchange: str) -> list[Path]:
    ohlc_dir = BASE / exchange / "ohlc"
    if not ohlc_dir.exists():
        return []
    return sorted(ohlc_dir.rglob("*.parquet"))


def main(dry_run: bool = True) -> None:
    for exchange, reason in TARGETS.items():
        files = _list_files(exchange)
        if not files:
            print(f"[{exchange}] no parquet files found — skipping")
            continue

        print(f"\n[{exchange}] {reason}")
        for f in files:
            size_kb = f.stat().st_size // 1024
            print(f"  {'(dry-run) ' if dry_run else ''}delete  {f.relative_to(BASE)}  ({size_kb} KB)")
            if not dry_run:
                f.unlink()

    if dry_run:
        print("\nDry-run complete. Re-run with --confirm to actually delete.")
    else:
        print("\nDone. Re-run the backfill:")
        print("  dccd backfill --config config.yml --exchange kraken --start '2024-01-01'")
        print("  dccd backfill --config config.yml --exchange okx    --start '2024-01-01'")


if __name__ == "__main__":
    main(dry_run="--confirm" not in sys.argv)
