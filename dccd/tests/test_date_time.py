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
    assert str_to_span('bi-hourly') == 7200
    assert str_to_span('2h') == 7200
    assert str_to_span('hourly') == 3600
    assert str_to_span('half-hourly') == 1800
    assert str_to_span('30min') == 1800
    assert str_to_span('5min') == 300
    assert str_to_span('minutely') == 60


def test_str_to_span_new_spans():
    assert str_to_span('monthly') == 2592000
    assert str_to_span('1M') == 2592000
    assert str_to_span('15d') == 1296000
    assert str_to_span('15-daily') == 1296000
    assert str_to_span('3d') == 259200
    assert str_to_span('3-day') == 259200
    assert str_to_span('12h') == 43200
    assert str_to_span('12-hourly') == 43200
    assert str_to_span('8h') == 28800
    assert str_to_span('8-hour') == 28800
    assert str_to_span('6h') == 21600
    assert str_to_span('6-hourly') == 21600
    assert str_to_span('4h') == 14400
    assert str_to_span('4-hour') == 14400
    assert str_to_span('quarter-hourly') == 900
    assert str_to_span('15min') == 900
    assert str_to_span('15m') == 900
    assert str_to_span('3m') == 180
    assert str_to_span('3min') == 180
    assert str_to_span('3-minute') == 180


def test_str_to_span_case_insensitive():
    assert str_to_span('MONTHLY') == 2592000
    assert str_to_span('Weekly') == 604800
    assert str_to_span('DAILY') == 86400
    assert str_to_span('Hourly') == 3600


def test_str_to_span_unknown_returns_none():
    assert str_to_span('fortnight') is None


def test_span_to_str_values():
    assert span_to_str(60) == 'Minutely'
    assert span_to_str(300) == 'Five_Minutely'
    assert span_to_str(1800) == 'Half_Hourly'
    assert span_to_str(3600) == 'Hourly'
    assert span_to_str(7200) == 'Bi_Hourly'
    assert span_to_str(86400) == 'Daily'
    assert span_to_str(604800) == 'Weekly'


def test_span_to_str_new_spans():
    assert span_to_str(180) == 'Three_Minutely'
    assert span_to_str(900) == 'Quarter_Hourly'
    assert span_to_str(14400) == 'Four_Hourly'
    assert span_to_str(21600) == 'Six_Hourly'
    assert span_to_str(28800) == 'Eight_Hourly'
    assert span_to_str(43200) == 'Twelve_Hourly'
    assert span_to_str(259200) == 'Three_Daily'
    assert span_to_str(1296000) == 'Fifteen_Daily'
    assert span_to_str(2592000) == 'Monthly'


def test_span_to_str_unknown_returns_none():
    assert span_to_str(999) is None


def test_span_roundtrip():
    spans = [
        60, 180, 300, 900, 1800, 3600, 7200, 14400,
        21600, 28800, 43200, 86400, 259200, 604800, 1296000, 2592000,
    ]
    for span in spans:
        label = span_to_str(span)
        assert label is not None, f"span_to_str({span}) returned None"


def test_binance_interval_minutes():
    assert binance_interval(60) == '1m'
    assert binance_interval(180) == '3m'
    assert binance_interval(300) == '5m'
    assert binance_interval(900) == '15m'
    assert binance_interval(1800) == '30m'


def test_binance_interval_hours():
    assert binance_interval(3600) == '1h'
    assert binance_interval(7200) == '2h'
    assert binance_interval(14400) == '4h'
    assert binance_interval(21600) == '6h'
    assert binance_interval(28800) == '8h'
    assert binance_interval(43200) == '12h'


def test_binance_interval_days():
    assert binance_interval(86400) == '1d'
    assert binance_interval(259200) == '3d'
    assert binance_interval(604800) == '1w'
    assert binance_interval(2592000) == '1M'


def test_binance_interval_unknown_returns_none():
    assert binance_interval(999) is None
