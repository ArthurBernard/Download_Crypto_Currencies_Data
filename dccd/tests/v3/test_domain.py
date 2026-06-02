"""Tests for the domain layer."""

import pytest
from dccd.domain.symbol import Symbol
from dccd.domain.types import DataType
from dccd.domain.records import OHLCBar, Trade, OrderBookLevel, OrderBookSnapshot
from dccd.domain.dataset import DatasetId, Provenance
from dccd.domain.capability import Capability
from dccd.domain.errors import NoCapability, CoverageError
from dccd.domain.timeutils import (
    NS, s_to_ns, ns_to_s, align_ns, str_to_span, span_label,
    binance_interval, bybit_interval, okx_interval, kraken_interval, coinbase_granularity,
)
from dccd.domain.transforms import aggregate_ohlc


# ---------------------------------------------------------------------------
# Symbol
# ---------------------------------------------------------------------------

class TestSymbol:
    def test_basic(self):
        s = Symbol(base="BTC", quote="USDT")
        assert str(s) == "BTC/USDT"

    def test_xbt_alias(self):
        s = Symbol(base="XBT", quote="USD")
        assert s.base == "BTC"

    def test_parse_slash(self):
        s = Symbol.parse("BTC/USDT")
        assert s.base == "BTC"
        assert s.quote == "USDT"

    def test_parse_dash(self):
        s = Symbol.parse("ETH-USD")
        assert s.base == "ETH"

    def test_parse_xbt_normalised(self):
        s = Symbol.parse("XBT/USD")
        assert s.base == "BTC"

    def test_parse_no_separator(self):
        with pytest.raises(ValueError):
            Symbol.parse("BTCUSDT")

    def test_frozen(self):
        s = Symbol(base="BTC", quote="USDT")
        with pytest.raises(Exception):
            s.base = "ETH"

    def test_hashable(self):
        s1 = Symbol(base="BTC", quote="USDT")
        s2 = Symbol(base="BTC", quote="USDT")
        assert hash(s1) == hash(s2)


# ---------------------------------------------------------------------------
# DataType
# ---------------------------------------------------------------------------

class TestDataType:
    def test_from_string(self):
        assert DataType("ohlc") == DataType.OHLC
        assert DataType("trades") == DataType.TRADES
        assert DataType("orderbook") == DataType.ORDERBOOK

    def test_invalid(self):
        with pytest.raises(ValueError):
            DataType("unknown")


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------

class TestRecords:
    def test_ohlc_bar(self):
        bar = OHLCBar(ts=1_000_000_000_000_000_000, open=1.0, high=2.0, low=0.5, close=1.5, volume=10.0)
        assert bar.ts == 1_000_000_000_000_000_000
        assert bar.high == 2.0

    def test_trade(self):
        t = Trade(ts=1_000_000_000_000_000_000, price=50000.0, amount=0.1, side="buy", tid="123")
        assert t.side == "buy"
        assert t.tid == "123"

    def test_orderbook_snapshot(self):
        snap = OrderBookSnapshot(
            ts=1_000_000_000_000_000_000,
            bids=[OrderBookLevel(price=50000.0, amount=1.0)],
            asks=[OrderBookLevel(price=50001.0, amount=0.5)],
        )
        assert snap.is_snapshot is True
        assert len(snap.bids) == 1


# ---------------------------------------------------------------------------
# DatasetId
# ---------------------------------------------------------------------------

class TestDatasetId:
    def test_basic(self):
        ds = DatasetId(
            exchange="binance",
            symbol=Symbol(base="BTC", quote="USDT"),
            data_type=DataType.OHLC,
            span=3600,
        )
        assert ds.exchange == "binance"
        assert ds.pair_slug() == "BTC-USDT"

    def test_frozen_hashable(self):
        ds = DatasetId(
            exchange="binance",
            symbol=Symbol(base="BTC", quote="USDT"),
            data_type=DataType.OHLC,
            span=3600,
        )
        {ds}  # hashable

    def test_provenance(self):
        ds = DatasetId(
            exchange="binance",
            symbol=Symbol(base="BTC", quote="USDT"),
            data_type=DataType.OHLC,
            span=3600,
        )
        prov = Provenance(source="binance:rest", derived_from=None)
        assert prov.source == "binance:rest"


# ---------------------------------------------------------------------------
# Timeutils
# ---------------------------------------------------------------------------

class TestTimeutils:
    def test_s_to_ns(self):
        assert s_to_ns(1548432099) == 1548432099_000_000_000

    def test_ns_to_s(self):
        assert ns_to_s(1548432099_000_000_000) == 1548432099.0

    def test_align(self):
        assert align_ns(3700 * NS, 3600) == 3600 * NS
        assert align_ns(3600 * NS, 3600) == 3600 * NS

    def test_str_to_span(self):
        assert str_to_span("1h") == 3600
        assert str_to_span("daily") == 86400
        assert str_to_span("1m") == 60

    def test_span_label(self):
        assert span_label(3600) == "1h"
        assert span_label(86400) == "1d"
        assert span_label(7777) == "7777s"

    def test_binance_interval(self):
        assert binance_interval(3600) == "1h"
        assert binance_interval(60) == "1m"
        assert binance_interval(86400) == "1d"

    def test_bybit_interval(self):
        assert bybit_interval(3600) == "60"
        assert bybit_interval(86400) == "D"

    def test_okx_interval(self):
        assert okx_interval(3600) == "1H"
        assert okx_interval(60) == "1m"

    def test_kraken_interval(self):
        assert kraken_interval(3600) == 60
        assert kraken_interval(86400) == 1440

    def test_coinbase_granularity(self):
        assert coinbase_granularity(3600) == 3600
        assert coinbase_granularity(7200) is None


# ---------------------------------------------------------------------------
# Transforms
# ---------------------------------------------------------------------------

class TestTransforms:
    def test_aggregate_empty(self):
        assert aggregate_ohlc([], 60) == []

    def test_aggregate_basic(self):
        trades = [
            Trade(ts=s_to_ns(0), price=100.0, amount=1.0, side="buy"),
            Trade(ts=s_to_ns(30), price=110.0, amount=2.0, side="sell"),
            Trade(ts=s_to_ns(60), price=105.0, amount=0.5, side="buy"),
        ]
        bars = aggregate_ohlc(trades, span=60)
        assert len(bars) == 2
        assert bars[0].open == 100.0
        assert bars[0].close == 110.0
        assert bars[0].high == 110.0
        assert bars[0].low == 100.0
        assert bars[0].volume == 3.0
        assert bars[1].open == 105.0

    def test_aggregate_single_trade(self):
        trades = [Trade(ts=s_to_ns(0), price=50000.0, amount=1.0)]
        bars = aggregate_ohlc(trades, span=3600)
        assert len(bars) == 1
        assert bars[0].high == 50000.0

    def test_aggregate_trade_count(self):
        trades = [
            Trade(ts=s_to_ns(0), price=100.0, amount=1.0),
            Trade(ts=s_to_ns(10), price=101.0, amount=1.0),
        ]
        bars = aggregate_ohlc(trades, span=60)
        assert bars[0].trades == 2


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

class TestErrors:
    def test_no_capability(self):
        err = NoCapability("kraken", "ohlc", "historical", "only 720 recent")
        assert "kraken" in str(err)
        assert "720 recent" in str(err)

    def test_coverage_error(self):
        err = CoverageError("binance", "trades", 0)
        assert "binance" in str(err)


# ---------------------------------------------------------------------------
# Capability
# ---------------------------------------------------------------------------

class TestCapability:
    def test_basic(self):
        cap = Capability(
            data_type=DataType.OHLC,
            transport="rest",
            mode="historical",
            max_per_request=1000,
        )
        assert cap.history == "full"
        assert cap.max_per_request == 1000

    def test_recent(self):
        cap = Capability(
            data_type=DataType.OHLC,
            transport="rest",
            mode="historical",
            history="recent",
            max_per_request=720,
        )
        assert cap.history == "recent"
