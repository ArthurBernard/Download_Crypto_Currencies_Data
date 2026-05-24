#!/usr/bin/env python3
# coding: utf-8

"""Tests for dccd.storage.DataStore."""

from __future__ import annotations

import polars as pl
import pytest

from dccd.storage import DataStore

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ohlc_df(ts_values: list[int]) -> pl.DataFrame:
    return pl.DataFrame({
        'TS': ts_values,
        'open': [100.0] * len(ts_values),
        'high': [101.0] * len(ts_values),
        'low': [99.0] * len(ts_values),
        'close': [100.5] * len(ts_values),
        'volume': [1.0] * len(ts_values),
    })


def _trades_df(ts_values: list[int]) -> pl.DataFrame:
    return pl.DataFrame({
        'TS': ts_values,
        'price': [50000.0] * len(ts_values),
        'amount': [0.1] * len(ts_values),
    })


# ---------------------------------------------------------------------------
# DataStore.directory — path structure
# ---------------------------------------------------------------------------

def test_ohlc_directory_structure(tmp_path):
    store = DataStore(str(tmp_path), 'binance', 'BTC/USDT', 3600, 'ohlc')
    expected = tmp_path / 'binance' / 'ohlc' / 'BTC-USDT' / '1h'
    assert store.directory == expected
    assert store.directory.exists()


def test_trades_directory_structure(tmp_path):
    store = DataStore(str(tmp_path), 'kraken', 'BTC/USD', None, 'trades')
    expected = tmp_path / 'kraken' / 'trades' / 'BTC-USD'
    assert store.directory == expected


def test_orderbook_directory_structure(tmp_path):
    store = DataStore(str(tmp_path), 'binance', 'ETH/USDT', None, 'orderbook')
    expected = tmp_path / 'binance' / 'orderbook' / 'ETH-USDT'
    assert store.directory == expected


def test_exchange_lowercased(tmp_path):
    store = DataStore(str(tmp_path), 'Binance', 'BTC/USDT', 60, 'ohlc')
    assert 'binance' in str(store.directory)


def test_invalid_data_type_raises():
    with pytest.raises(ValueError, match='data_type must be'):
        DataStore('/tmp', 'binance', 'BTC/USDT', 3600, 'invalid')


def test_ohlc_without_span_raises():
    with pytest.raises(ValueError, match='span is required'):
        DataStore('/tmp', 'binance', 'BTC/USDT', None, 'ohlc')


# ---------------------------------------------------------------------------
# DataStore.save — OHLC annual files
# ---------------------------------------------------------------------------

def test_save_ohlc_creates_annual_file(tmp_path):
    store = DataStore(str(tmp_path), 'binance', 'BTC/USDT', 3600, 'ohlc')
    # TS in year 2023
    df = _ohlc_df([1672531200, 1675209600])  # 2023-01-01, 2023-02-01
    store.save(df)

    files = list(store.directory.glob('*.parquet'))
    assert len(files) == 1
    assert files[0].stem == '2023'


def test_save_ohlc_multiple_years(tmp_path):
    store = DataStore(str(tmp_path), 'binance', 'BTC/USDT', 3600, 'ohlc')
    df = _ohlc_df([
        1672531200,  # 2023-01-01
        1704067200,  # 2024-01-01
    ])
    store.save(df)

    files = sorted(store.directory.glob('*.parquet'))
    assert len(files) == 2
    assert {f.stem for f in files} == {'2023', '2024'}


def test_save_merges_with_existing(tmp_path):
    store = DataStore(str(tmp_path), 'binance', 'BTC/USDT', 3600, 'ohlc')
    df1 = _ohlc_df([1672531200, 1672534800])  # 2023-01-01 00:00, 01:00
    df2 = _ohlc_df([1672534800, 1672538400])  # 2023-01-01 01:00 (dup), 02:00
    store.save(df1)
    store.save(df2)

    result = pl.read_parquet(store.directory / '2023.parquet')
    # Dedup on TS: 3 unique timestamps
    assert len(result) == 3
    assert sorted(result['TS'].to_list()) == [1672531200, 1672534800, 1672538400]



def test_save_empty_df_is_noop(tmp_path):
    store = DataStore(str(tmp_path), 'binance', 'BTC/USDT', 3600, 'ohlc')
    store.save(pl.DataFrame())
    assert list(store.directory.glob('*.parquet')) == []


# ---------------------------------------------------------------------------
# DataStore.save — trades daily files
# ---------------------------------------------------------------------------

def test_save_trades_daily_file(tmp_path):
    store = DataStore(str(tmp_path), 'binance', 'BTC/USDT', None, 'trades')
    df = _trades_df([1700000000, 1700003600])  # both 2023-11-14
    store.save(df)

    files = list(store.directory.glob('*.parquet'))
    assert len(files) == 1
    assert files[0].stem == '2023-11-14'


def test_save_trades_multiple_days(tmp_path):
    store = DataStore(str(tmp_path), 'binance', 'BTC/USDT', None, 'trades')
    df = _trades_df([
        1700000000,  # 2023-11-14
        1700100000,  # 2023-11-15
    ])
    store.save(df)

    files = sorted(store.directory.glob('*.parquet'))
    assert len(files) == 2


# ---------------------------------------------------------------------------
# DataStore.load
# ---------------------------------------------------------------------------

def test_load_returns_empty_when_no_data(tmp_path):
    store = DataStore(str(tmp_path), 'binance', 'BTC/USDT', 3600, 'ohlc')
    result = store.load()
    assert len(result) == 0


def test_load_range_across_years(tmp_path):
    store = DataStore(str(tmp_path), 'binance', 'BTC/USDT', 3600, 'ohlc')
    store.save(_ohlc_df([1672531200, 1704067200]))  # 2023 and 2024

    result = store.load()
    assert len(result) == 2

    result_filtered = store.load(start=1672531200, end=1672531200)
    assert len(result_filtered) == 1
    assert result_filtered['TS'][0] == 1672531200


def test_load_skips_corrupted_file(tmp_path):
    store = DataStore(str(tmp_path), 'binance', 'BTC/USDT', 3600, 'ohlc')
    store.save(_ohlc_df([1672531200]))  # writes 2023.parquet
    (store.directory / '2022.parquet').write_bytes(b'bad data')

    result = store.load()
    assert len(result) == 1  # only 2023 loaded


# ---------------------------------------------------------------------------
# DataStore.existing_periods
# ---------------------------------------------------------------------------

def test_existing_periods_empty(tmp_path):
    store = DataStore(str(tmp_path), 'binance', 'BTC/USDT', 3600, 'ohlc')
    assert store.existing_periods() == []


def test_existing_periods_ohlc(tmp_path):
    store = DataStore(str(tmp_path), 'binance', 'BTC/USDT', 3600, 'ohlc')
    store.save(_ohlc_df([1672531200, 1704067200]))  # 2023 and 2024
    assert store.existing_periods() == ['2023', '2024']


# ---------------------------------------------------------------------------
# DataStore.last_timestamp
# ---------------------------------------------------------------------------

def test_last_timestamp_empty(tmp_path):
    store = DataStore(str(tmp_path), 'binance', 'BTC/USDT', 3600, 'ohlc')
    assert store.last_timestamp() is None


def test_last_timestamp_with_data(tmp_path):
    store = DataStore(str(tmp_path), 'binance', 'BTC/USDT', 3600, 'ohlc')
    store.save(_ohlc_df([1672531200, 1704067200]))
    assert store.last_timestamp() == 1704067200


def test_last_timestamp_removes_corrupted_and_falls_back(tmp_path):
    store = DataStore(str(tmp_path), 'binance', 'BTC/USDT', 3600, 'ohlc')
    store.save(_ohlc_df([1672531200]))  # 2023.parquet
    bad = store.directory / '2024.parquet'
    bad.write_bytes(b'corrupted')

    ts = store.last_timestamp()
    assert ts == 1672531200  # falls back to 2023
    assert not bad.exists()  # corrupted file was removed


# ---------------------------------------------------------------------------
# Helpers for gap-detection tests
# ---------------------------------------------------------------------------

def _complete_year_df(year: int, span: int) -> pl.DataFrame:
    from datetime import datetime, timezone
    t0 = int(datetime(year,     1, 1, tzinfo=timezone.utc).timestamp())
    t1 = int(datetime(year + 1, 1, 1, tzinfo=timezone.utc).timestamp())
    return _ohlc_df(list(range(t0, t1, span)))


# ---------------------------------------------------------------------------
# DataStore.is_period_complete
# ---------------------------------------------------------------------------

def test_is_period_complete_true(tmp_path):
    span = 3600
    store = DataStore(str(tmp_path), 'binance', 'BTC/USDT', span, 'ohlc')
    store.save(_complete_year_df(2023, span))
    assert store.is_period_complete(2023) is True


def test_is_period_complete_false_missing_file(tmp_path):
    store = DataStore(str(tmp_path), 'binance', 'BTC/USDT', 3600, 'ohlc')
    assert store.is_period_complete(2023) is False


def test_is_period_complete_false_partial(tmp_path):
    store = DataStore(str(tmp_path), 'binance', 'BTC/USDT', 3600, 'ohlc')
    store.save(_ohlc_df([1672531200]))  # one row only
    assert store.is_period_complete(2023) is False


def test_is_period_complete_false_for_trades(tmp_path):
    store = DataStore(str(tmp_path), 'binance', 'BTC/USDT', None, 'trades')
    assert store.is_period_complete(2023) is False


# ---------------------------------------------------------------------------
# DataStore.missing_intervals
# ---------------------------------------------------------------------------

# Boundaries used in tests:
#   2023-01-01 00:00:00 UTC = 1672531200
#   2024-01-01 00:00:00 UTC = 1704067200
#   2025-01-01 00:00:00 UTC = 1735689600

_Y2023 = 1672531200
_Y2024 = 1704067200
_Y2025 = 1735689600


def test_missing_intervals_no_data(tmp_path):
    store = DataStore(str(tmp_path), 'binance', 'BTC/USDT', 3600, 'ohlc')
    intervals = store.missing_intervals(_Y2023, _Y2024)
    assert intervals == [(_Y2023, _Y2024)]


def test_missing_intervals_complete_past_year(tmp_path):
    span = 3600
    store = DataStore(str(tmp_path), 'binance', 'BTC/USDT', span, 'ohlc')
    store.save(_complete_year_df(2023, span))
    intervals = store.missing_intervals(_Y2023, _Y2024)
    assert intervals == []


def test_missing_intervals_incomplete_past_year(tmp_path):
    span = 3600
    store = DataStore(str(tmp_path), 'binance', 'BTC/USDT', span, 'ohlc')
    last_ts = _Y2023 + 10 * span
    store.save(_ohlc_df(list(range(_Y2023, last_ts + span, span))))
    intervals = store.missing_intervals(_Y2023, _Y2024)
    assert len(intervals) == 1
    assert intervals[0][0] == last_ts + span
    assert intervals[0][1] == _Y2024


def test_missing_intervals_absent_past_year(tmp_path):
    store = DataStore(str(tmp_path), 'binance', 'BTC/USDT', 3600, 'ohlc')
    # 2023 file absent: full year should be included
    intervals = store.missing_intervals(_Y2023, _Y2024)
    assert intervals == [(_Y2023, _Y2024)]


def test_missing_intervals_current_year_extends_from_last(tmp_path):
    from datetime import datetime, timezone
    span = 3600
    store = DataStore(str(tmp_path), 'binance', 'BTC/USDT', span, 'ohlc')
    current_year = datetime.now(tz=timezone.utc).year
    year_start = int(datetime(current_year, 1, 1, tzinfo=timezone.utc).timestamp())
    last_ts = year_start + 5 * span
    store.save(_ohlc_df(list(range(year_start, last_ts + span, span))))
    end_ts = last_ts + 100 * span
    intervals = store.missing_intervals(year_start, end_ts)
    assert len(intervals) == 1
    assert intervals[0][0] == last_ts + span
    assert intervals[0][1] == end_ts


def test_missing_intervals_multi_year(tmp_path):
    span = 3600
    store = DataStore(str(tmp_path), 'binance', 'BTC/USDT', span, 'ohlc')
    # 2023 complete → skip; 2024 partial → resume
    store.save(_complete_year_df(2023, span))
    last_2024 = _Y2024 + 10 * span
    store.save(_ohlc_df(list(range(_Y2024, last_2024 + span, span))))
    intervals = store.missing_intervals(_Y2023, _Y2025)
    # only one interval: the tail of 2024 through end of 2025
    assert len(intervals) == 1
    assert intervals[0][0] == last_2024 + span


def test_missing_intervals_already_up_to_date(tmp_path):
    span = 3600
    store = DataStore(str(tmp_path), 'binance', 'BTC/USDT', span, 'ohlc')
    store.save(_complete_year_df(2023, span))
    intervals = store.missing_intervals(_Y2023, _Y2024)
    assert intervals == []


def test_missing_intervals_start_before_file_min_current_year(tmp_path):
    """File starts mid-year; requested start is before file_min → beginning gap + trailing gap."""
    from datetime import datetime, timezone
    span = 3600
    store = DataStore(str(tmp_path), 'binance', 'BTC/USDT', span, 'ohlc')
    current_year = datetime.now(tz=timezone.utc).year
    year_start = int(datetime(current_year, 1, 1, tzinfo=timezone.utc).timestamp())
    # File covers mid-May onwards (simulates the user's real situation)
    file_start = year_start + 100 * span
    file_end   = year_start + 110 * span
    store.save(_ohlc_df(list(range(file_start, file_end + span, span))))
    end_ts = file_end + 50 * span

    intervals = store.missing_intervals(year_start, end_ts)

    # Expect two intervals: [year_start, file_start) and [file_end+span, end_ts)
    assert len(intervals) == 2
    assert intervals[0] == (year_start, file_start)
    assert intervals[1] == (file_end + span, end_ts)


def test_missing_intervals_start_before_file_min_past_year(tmp_path):
    """Past incomplete year where file starts after requested start → beginning gap returned."""
    span = 3600
    store = DataStore(str(tmp_path), 'binance', 'BTC/USDT', span, 'ohlc')
    file_start = _Y2023 + 100 * span
    file_end   = _Y2023 + 200 * span
    store.save(_ohlc_df(list(range(file_start, file_end + span, span))))

    intervals = store.missing_intervals(_Y2023, _Y2024)

    assert len(intervals) == 2
    assert intervals[0] == (_Y2023, file_start)
    assert intervals[1] == (file_end + span, _Y2024)


def test_missing_intervals_file_already_covers_full_request(tmp_path):
    """File already covers [start, end] exactly → no intervals."""
    span = 3600
    store = DataStore(str(tmp_path), 'binance', 'BTC/USDT', span, 'ohlc')
    store.save(_ohlc_df(list(range(_Y2023, _Y2023 + 10 * span + span, span))))
    end_ts = _Y2023 + 5 * span
    intervals = store.missing_intervals(_Y2023, end_ts)
    assert intervals == []


def test_missing_intervals_non_ohlc_simple_resume(tmp_path):
    store = DataStore(str(tmp_path), 'binance', 'BTC/USDT', None, 'trades')
    store.save(_trades_df([1672531200]))
    intervals = store.missing_intervals(1672531200, 1704067200)
    assert len(intervals) == 1
    # no span for trades — stub returns from last_timestamp (no +span)
    assert intervals[0][1] == 1704067200
