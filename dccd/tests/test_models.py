#!/usr/bin/env python3
# coding: utf-8

import pytest
from pydantic import ValidationError

from dccd.models import OHLCBar, OrderBookEntry, Trade


def test_ohlcbar_valid():
    bar = OHLCBar(date=1.0, open=50000.0, high=51000.0, low=49000.0,
                  close=50500.0, volume=100.0, quoteVolume=5050000.0)
    assert bar.close == 50500.0
    assert bar.weightedAverage is None


def test_ohlcbar_coercion():
    bar = OHLCBar(date='1.0', open='50000', high='51000', low='49000',
                  close='50500', volume='100', quoteVolume='5050000')
    assert isinstance(bar.open, float)


def test_ohlcbar_missing_field():
    with pytest.raises(ValidationError):
        OHLCBar(date=1.0, open=50000.0, high=51000.0, low=49000.0,
                close=50500.0, volume=100.0)


def test_ohlcbar_weighted_average():
    bar = OHLCBar(date=1.0, open=1.0, high=1.0, low=1.0, close=1.0,
                  volume=1.0, quoteVolume=1.0, weightedAverage=1.0)
    assert bar.weightedAverage == 1.0


def test_trade_valid():
    t = Trade(tid=1, timestamp=1.0, price=50000.0, amount=0.5, type='buy')
    assert t.type == 'buy'


def test_trade_type_optional():
    t = Trade(tid=1, timestamp=1.0, price=50000.0, amount=0.5)
    assert t.type is None


def test_orderbookentry_valid():
    e = OrderBookEntry(price='50000', count=3, amount=1.5)
    assert e.price == '50000'
