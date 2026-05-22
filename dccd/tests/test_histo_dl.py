#!/usr/bin/env python3
# coding: utf-8

import pathlib

import pandas as pd

_FALLBACK_TS = 1325376000  # 2012-01-01 00:00:00 UTC


def _sample_df() -> pd.DataFrame:
    return pd.DataFrame({'TS': [1700000000, 1700003600, 1700007200]})


# ---------------------------------------------------------------------------
# _get_last_date — delegates to DataStore.last_timestamp
# ---------------------------------------------------------------------------

def test_get_last_date_empty_directory(tmp_path):
    from dccd.histo_dl.binance import FromBinance

    obj = FromBinance(str(tmp_path), 'BTC', 60, fiat='USDT')
    assert obj._get_last_date() == _FALLBACK_TS


def test_get_last_date_parquet(tmp_path):
    from dccd.histo_dl.binance import FromBinance

    obj = FromBinance(str(tmp_path), 'BTC', 60, fiat='USDT')
    df = _sample_df()
    df.to_parquet(pathlib.Path(obj.full_path) / '2023.parquet', index=False)
    assert obj._get_last_date() == 1700007200


def test_get_last_date_corrupted_parquet(tmp_path):
    from dccd.histo_dl.binance import FromBinance

    obj = FromBinance(str(tmp_path), 'BTC', 60, fiat='USDT')
    bad = pathlib.Path(obj.full_path) / '2023.parquet'
    bad.write_bytes(b'not-a-parquet-file')
    assert obj._get_last_date() == _FALLBACK_TS
    assert not bad.exists()


# ---------------------------------------------------------------------------
# save — delegates to DataStore, annual Parquet under new arborescence
# ---------------------------------------------------------------------------

def test_save_parquet(tmp_path):
    from dccd.histo_dl.binance import FromBinance

    obj = FromBinance(str(tmp_path), 'BTC', 60, fiat='USDT')
    data = [{
        'date': 1700000000.0, 'open': 37000.0, 'high': 37010.0,
        'low': 36990.0, 'close': 37005.0, 'volume': 1.5,
        'quoteVolume': 55507.5,
    }]
    obj._sort_data(data)
    obj.save()

    files = list(pathlib.Path(obj.full_path).glob('*.parquet'))
    assert len(files) == 1
    df = pd.read_parquet(files[0])
    assert 'TS' in df.columns
    assert len(df) >= 1


def test_save_creates_correct_path(tmp_path):
    from dccd.histo_dl.binance import FromBinance

    obj = FromBinance(str(tmp_path), 'BTC', 60, fiat='USDT')
    # New arborescence: {data_path}/binance/ohlc/BTC-USDT/1m/
    assert 'binance' in obj.full_path
    assert 'ohlc' in obj.full_path
    assert 'BTC-USDT' in obj.full_path
    assert '1m' in obj.full_path
