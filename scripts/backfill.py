#!/usr/bin/env python3
"""Historical OHLC backfill for Binance, Kraken, and Bybit.

Fetches 1-minute candles in rolling windows to stay within each exchange's
API limit, then saves to Parquet files grouped by month.

Usage
-----
    python scripts/backfill.py

Adjust PATH, START, PAIRS, and SPAN below before running.

API limits (1-minute candles)
------------------------------
- Binance  : 1 000 candles per request  → window ≈ 16 h
- Kraken   : 720 candles per request   → window = 12 h
- Bybit    : 1 000 candles per request  → window ≈ 16 h
"""

from __future__ import annotations

import time as time_mod

from dccd.histo_dl import FromBinance, FromKraken, FromBybit
from dccd.tools.date_time import date_to_TS

# ---------------------------------------------------------------------------
# Configuration — edit these
# ---------------------------------------------------------------------------
PATH = '/data/crypto'      # root storage directory
SPAN = 60                  # candle interval in seconds
START = '2020-01-01 00:00:00'
FORM = 'parquet'
BY_PERIOD = 'M'            # one file per month

JOBS: list[tuple] = [
    # (exchange_class, crypto, fiat, max_candles_per_request)
    (FromBinance, 'BTC',  'USDT', 990),
    (FromBinance, 'ETH',  'USDT', 990),
    (FromKraken,  'BTC',  'USD',  710),
    (FromKraken,  'ETH',  'USD',  710),
    (FromBybit,   'BTC',  'USDT', 990),
    (FromBybit,   'ETH',  'USDT', 990),
]

SLEEP_BETWEEN_REQUESTS = 0.35  # seconds — stay well under rate limits
# ---------------------------------------------------------------------------


def windowed_backfill(
    cls, path: str, crypto: str, fiat: str, span: int,
    start_str: str, max_candles: int = 990,
    form: str = 'parquet', by_period: str = 'M',
) -> None:
    """Download full OHLC history in rolling windows and save incrementally."""
    window = max_candles * span
    now_ts = int(time_mod.time())

    obj = cls(path, crypto, span, fiat=fiat)
    label = f'{cls.__name__} {crypto}/{fiat}'

    # Resume from last saved point if data already exists.
    last_saved = obj._get_last_date()
    user_start = int(date_to_TS(start_str))
    current = max(user_start, last_saved)

    if current >= now_ts:
        print(f'  {label}: already up to date')
        return

    n_windows = 0
    n_candles = 0

    while current < now_ts:
        end = min(current + window, now_ts)

        # Reload last_df from disk so save() can merge without data loss.
        obj._get_last_date()

        obj.import_data(start=current, end=end)
        obj.save(form=form, by_period=by_period)

        batch = len(obj.df) if obj.df is not None else 0
        n_candles += batch
        n_windows += 1

        current = obj.end + obj.span

        print(f'  {label}: window {n_windows} — up to {obj.end} ({batch} rows)')
        time_mod.sleep(SLEEP_BETWEEN_REQUESTS)

    print(f'  {label}: done — {n_windows} windows, {n_candles} total rows\n')


def main() -> None:
    for cls, crypto, fiat, max_candles in JOBS:
        print(f'Starting backfill: {cls.__name__} {crypto}/{fiat}')
        try:
            windowed_backfill(
                cls, PATH, crypto, fiat, SPAN, START,
                max_candles=max_candles, form=FORM, by_period=BY_PERIOD,
            )
        except Exception as exc:
            print(f'  ERROR: {exc}\n')


if __name__ == '__main__':
    main()
