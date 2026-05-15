#!/usr/bin/env python3
# coding: utf-8


import pandas as pd

from dccd.histo_dl.exchange import ImportDataCryptoCurrencies

_FALLBACK_TS = 1325376000  # 2012-01-01 00:00:00 UTC


def _make_obj(full_path: str) -> ImportDataCryptoCurrencies:
    obj = ImportDataCryptoCurrencies.__new__(ImportDataCryptoCurrencies)
    import logging
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
