#!/usr/bin/env python3
# coding: utf-8

import pandas as pd

from dccd.process_data import set_marketdepth, set_ohlc, set_orders, set_trades

_TS = 1746057600  # 2025-05-01 00:00:00 UTC


def _trades():
    return [
        {'tid': 2, 'price': 50100.0, 'amount': 0.5, 'timestamp': (_TS + 30) * 1000},
        {'tid': 1, 'price': 50000.0, 'amount': 1.0, 'timestamp': _TS * 1000},
    ]


def _book():
    return {'50000': 1.0, '49900': 0.5, '-50100': -0.8, '-50200': -0.3}


def test_set_trades_sorted():
    result = set_trades(_trades())
    assert isinstance(result, pd.DataFrame)
    assert list(result['tid']) == [1, 2]


def test_set_orders_timestamp():
    orders = [{'price': 50000.0, 'qty': 1.0}, {'price': 50100.0, 'qty': 0.5}]
    result = set_orders(orders, t=_TS)
    assert isinstance(result, pd.DataFrame)
    assert 'timestamp' in result.columns
    assert (result['timestamp'] == _TS).all()


def test_set_orders_default_timestamp():
    import time
    orders = [{'price': 50000.0, 'qty': 1.0}]
    before = int(time.time())
    result = set_orders(orders)
    after = int(time.time())
    assert before <= int(result['timestamp'].iloc[0]) <= after


def test_set_ohlc_structure():
    result = set_ohlc(_trades(), ts=60)
    assert isinstance(result, pd.DataFrame)
    for col in ['open', 'high', 'low', 'close', 'volume']:
        assert col in result.columns


def test_set_ohlc_values():
    result = set_ohlc(_trades(), ts=86400)
    # Both trades fall in the same daily bucket
    row = result.dropna().iloc[0]
    assert float(row['open']) == 50000.0
    assert float(row['high']) == 50100.0
    assert float(row['low']) == 50000.0
    assert float(row['close']) == 50100.0
    assert float(row['volume']) == 1.5


def test_set_marketdepth_structure():
    result = set_marketdepth(_book(), t=_TS)
    assert isinstance(result, pd.DataFrame)
    index_values = result.index.get_level_values(1).unique().tolist()
    assert 'bid' in index_values
    assert 'ask' in index_values


def test_set_marketdepth_columns():
    result = set_marketdepth(_book(), t=_TS)
    col_names = result.index.get_level_values(2).unique().tolist()
    assert 'price' in col_names
    assert 'cum_amount' in col_names
    assert 'vwab' in col_names
