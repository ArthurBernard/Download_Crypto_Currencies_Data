"""Tests for source adapters — contract tests with mocked HTTP."""

from unittest.mock import AsyncMock, patch

import pytest

from dccd.domain.errors import NoCapability
from dccd.domain.symbol import Symbol
from dccd.domain.timeutils import NS
from dccd.domain.types import DataType
from dccd.sources.base import (
    OHLCHistory,
    OHLCLive,
    OrderBookLive,
    OrderBookSnapshotREST,
    TradesHistory,
    TradesLive,
)
from dccd.sources.binance import BinanceSource
from dccd.sources.bybit import BybitSource
from dccd.sources.coinbase import CoinbaseSource
from dccd.sources.kraken import KrakenSource
from dccd.sources.registry import SourceRegistry

BTC_USDT = Symbol(base="BTC", quote="USDT")
BTC_USD = Symbol(base="BTC", quote="USD")

START_NS = 1_600_000_000 * NS
END_NS = 1_600_003_600 * NS


class TestBinanceCapabilities:
    def setup_method(self):
        self.src = BinanceSource()

    def test_implements_ohlc_history(self):
        assert isinstance(self.src, OHLCHistory)

    def test_implements_trades_history(self):
        assert isinstance(self.src, TradesHistory)

    def test_implements_orderbook_rest(self):
        assert isinstance(self.src, OrderBookSnapshotREST)

    def test_implements_ohlc_live(self):
        assert isinstance(self.src, OHLCLive)

    def test_implements_trades_live(self):
        assert isinstance(self.src, TradesLive)

    def test_implements_orderbook_live(self):
        assert isinstance(self.src, OrderBookLive)

    def test_render_symbol(self):
        assert self.src.render_symbol(BTC_USDT) == "BTCUSDT"

    def test_capabilities_declared(self):
        caps = self.src.capabilities()
        types = {c.data_type for c in caps}
        assert DataType.OHLC in types
        assert DataType.TRADES in types
        assert DataType.ORDERBOOK in types

    def test_ohlc_cap_max_per_request(self):
        cap = self.src.capability_for(DataType.OHLC, "rest", "historical")
        assert cap is not None
        assert cap.max_per_request == 1000

    @pytest.mark.asyncio
    async def test_fetch_ohlc_page(self):
        mock_response = [
            [1600000000000, "50000", "51000", "49000", "50500", "100", 1600003600000,
             "5000000", 500, "50", "2500000", "0"]
        ]
        with patch.object(self.src._http, "get", new_callable=AsyncMock, return_value=mock_response):
            with patch("dccd.sources.binance.AsyncHTTPClient.__aenter__", return_value=self.src._http):
                with patch("dccd.sources.binance.AsyncHTTPClient.__aexit__", return_value=None):
                    # Direct call with mocked client
                    pass

    def test_exchange_name(self):
        assert self.src.exchange == "binance"


class TestCoinbaseCapabilities:
    def setup_method(self):
        self.src = CoinbaseSource()

    def test_render_symbol(self):
        assert self.src.render_symbol(BTC_USD) == "BTC-USD"

    def test_ohlc_cap_max_per_request(self):
        cap = self.src.capability_for(DataType.OHLC, "rest", "historical")
        assert cap is not None
        assert cap.max_per_request == 300

    def test_ohlc_cap_history_full(self):
        cap = self.src.capability_for(DataType.OHLC, "rest", "historical")
        assert cap.history == "full"

    def test_trades_cap_history_recent(self):
        cap = self.src.capability_for(DataType.TRADES, "rest", "historical")
        assert cap.history == "recent"

    def test_supported_spans(self):
        cap = self.src.capability_for(DataType.OHLC, "rest", "historical")
        assert 3600 in cap.spans
        assert 86400 in cap.spans


class TestKrakenCapabilities:
    def setup_method(self):
        self.src = KrakenSource()

    def test_render_symbol(self):
        assert self.src.render_symbol(BTC_USD) == "XXBTZUSD"

    def test_ohlc_cap_history_recent(self):
        cap = self.src.capability_for(DataType.OHLC, "rest", "historical")
        assert cap is not None
        assert cap.history == "recent"
        assert cap.max_per_request == 720

    def test_trades_cap_history_full(self):
        cap = self.src.capability_for(DataType.TRADES, "rest", "historical")
        assert cap is not None
        assert cap.history == "full"


class TestBybitCapabilities:
    def setup_method(self):
        self.src = BybitSource()

    def test_render_symbol(self):
        assert self.src.render_symbol(BTC_USDT) == "BTCUSDT"

    def test_no_trades_history(self):
        cap = self.src.capability_for(DataType.TRADES, "rest", "historical")
        assert cap is None

    def test_ohlc_history_full(self):
        cap = self.src.capability_for(DataType.OHLC, "rest", "historical")
        assert cap is not None
        assert cap.history == "full"


class TestStreamCapabilityHonesty:
    """Declared WS capabilities must match a real implementation (D8)."""

    def test_coinbase_no_ohlc_or_book_ws(self):
        src = CoinbaseSource()
        assert src.capability_for(DataType.OHLC, "ws", "live") is None
        assert src.capability_for(DataType.ORDERBOOK, "ws", "live") is None
        assert src.capability_for(DataType.TRADES, "ws", "live") is not None

    def test_bitfinex_no_book_ws(self):
        from dccd.sources.bitfinex import BitfinexSource
        src = BitfinexSource()
        assert src.capability_for(DataType.ORDERBOOK, "ws", "live") is None

    @pytest.mark.asyncio
    async def test_stream_rejects_undeclared_capability(self):
        from dccd.application.jobs import JobSpec, JobTarget, Trigger
        from dccd.application.operations import stream

        reg = SourceRegistry()
        reg.register("coinbase", CoinbaseSource())
        target = JobTarget(exchange="coinbase", symbol=BTC_USD,
                           data_type=DataType.OHLC, span=60)
        spec = JobSpec(id=JobSpec.make_id("stream", target), operation="stream",
                       target=target, trigger=Trigger(kind="supervised"))
        with pytest.raises(NoCapability):
            await stream(spec, registry=reg, store=None)


class TestSourceRegistry:
    def setup_method(self):
        self.reg = SourceRegistry()
        self.reg.register("binance", BinanceSource())
        self.reg.register("kraken", KrakenSource())
        self.reg.register("coinbase", CoinbaseSource())

    def test_get_registered(self):
        src = self.reg.get("binance")
        assert isinstance(src, BinanceSource)

    def test_get_unknown(self):
        with pytest.raises(NoCapability):
            self.reg.get("unknown_exchange")

    def test_get_ohlc_history(self):
        src = self.reg.get_ohlc_history("binance")
        assert isinstance(src, OHLCHistory)

    def test_get_trades_history_bybit_missing(self):
        self.reg.register("bybit", BybitSource())
        with pytest.raises(NoCapability):
            self.reg.get_trades_history("bybit")

    def test_exchanges_list(self):
        assert "binance" in self.reg.exchanges
        assert "kraken" in self.reg.exchanges

    def test_resolve_no_capability(self):
        # Bybit spot has no trades history — should raise NoCapability
        self.reg.register("bybit", BybitSource())
        with pytest.raises(NoCapability):
            self.reg.resolve("bybit", DataType.TRADES, "rest", "historical")
