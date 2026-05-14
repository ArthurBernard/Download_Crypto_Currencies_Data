#!/usr/bin/env python3
# coding: utf-8

from dccd.tools.date_time import (
    TS_to_date,
    binance_interval,
    date_to_TS,
    span_to_str,
    str_to_span,
)


def test_date_to_TS_roundtrip():
    ts = date_to_TS('2019-01-25 16:01:39')
    assert isinstance(ts, int)
    assert TS_to_date(ts, local=True) == '2019-01-25 16:01:39'


def test_str_to_span_aliases():
    for alias in ['weekly', 'week', '7d', '1w', 'w']:
        assert str_to_span(alias) == 604800
    assert str_to_span('daily') == 86400
    assert str_to_span('hourly') == 3600
    assert str_to_span('minutely') == 60


def test_span_to_str_values():
    assert span_to_str(60) == 'Minutely'
    assert span_to_str(3600) == 'Hourly'
    assert span_to_str(86400) == 'Daily'
    assert span_to_str(604800) == 'Weekly'


def test_binance_interval_minutes():
    assert binance_interval(60) == '1m'
    assert binance_interval(300) == '5m'


def test_binance_interval_hours():
    assert binance_interval(3600) == '1h'
    assert binance_interval(7200) == '2h'


def test_binance_interval_days():
    assert binance_interval(86400) == '1d'
    assert binance_interval(604800) == '1w'
