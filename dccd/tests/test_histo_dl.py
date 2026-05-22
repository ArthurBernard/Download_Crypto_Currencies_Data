#!/usr/bin/env python3
# coding: utf-8


import logging
import pathlib

import pandas as pd

from dccd.histo_dl.exchange import ImportDataCryptoCurrencies

_FALLBACK_TS = 1325376000  # 2012-01-01 00:00:00 UTC


class _ConcreteDownloader(ImportDataCryptoCurrencies):
    def _import_data(self, start, end):
        return []


def _make_obj(full_path: str) -> ImportDataCryptoCurrencies:
    obj = _ConcreteDownloader.__new__(_ConcreteDownloader)
    obj.logger = logging.getLogger(__name__)
    obj.last_df = pd.DataFrame()
    obj.full_path = full_path
    return obj


def _sample_df() -> pd.DataFrame:
    return pd.DataFrame({'TS': [1700000000, 1700003600, 1700007200]})


def test_get_last_date_empty_directory(tmp_path):
    obj = _make_obj(str(tmp_path))
    assert obj._get_last_date() == _FALLBACK_TS


def test_get_last_date_xlsx(tmp_path):
    df = _sample_df()
    df.to_excel(tmp_path / 'data_2023.xlsx', index=False)
    obj = _make_obj(str(tmp_path))
    assert obj._get_last_date() == 1700007200


def test_get_last_date_csv(tmp_path):
    df = _sample_df()
    df.to_csv(tmp_path / 'data_2023.csv', index=False)
    obj = _make_obj(str(tmp_path))
    assert obj._get_last_date() == 1700007200


def test_get_last_date_parquet(tmp_path):
    df = _sample_df()
    df.to_parquet(tmp_path / 'data_2023.parquet', index=False)
    obj = _make_obj(str(tmp_path))
    assert obj._get_last_date() == 1700007200


def test_get_last_date_unsupported_format(tmp_path):
    (tmp_path / 'data.json').write_text('{}')
    obj = _make_obj(str(tmp_path))
    assert obj._get_last_date() == _FALLBACK_TS


def test_save_parquet(tmp_path):
    from dccd.histo_dl.binance import FromBinance

    obj = FromBinance(str(tmp_path), 'BTC', 60, fiat='USDT')
    obj.last_df = pd.DataFrame()
    data = [{
        'date': 1700000000.0, 'open': 37000.0, 'high': 37010.0,
        'low': 36990.0, 'close': 37005.0, 'volume': 1.5,
        'quoteVolume': 55507.5,
    }]
    obj._sort_data(data)
    obj.save(form='parquet', by_period='Y')

    files = list(pathlib.Path(obj.full_path).glob('*.parquet'))
    assert len(files) == 1
    df = pd.read_parquet(files[0])
    assert 'TS' in df.columns
    assert len(df) >= 1
