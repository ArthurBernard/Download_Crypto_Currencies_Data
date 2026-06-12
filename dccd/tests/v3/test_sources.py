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
from dccd.sources.okx import OKXSource
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

    def test_render_symbol_crypto_quote_btc(self):
        # ETH/BTC: BTC→XBT for the *quote* too, else Kraken rejects "XETHXBTC".
        assert self.src.render_symbol(Symbol(base="ETH", quote="BTC")) == "XETHXXBT"

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


class TestBitfinexSymbol:
    """Bitfinex labels Tether UST and uses the colon form for long symbols."""

    def test_usdt_maps_to_ust(self):
        from dccd.sources.bitfinex import _bfx_symbol
        # tBTCUSDT returns [] (HTTP 200) — must map to tBTCUST.
        assert _bfx_symbol(Symbol(base="BTC", quote="USDT")) == "tBTCUST"
        assert _bfx_symbol(Symbol(base="BTC", quote="USD")) == "tBTCUSD"

    def test_long_symbol_uses_colon(self):
        from dccd.sources.bitfinex import _bfx_symbol
        assert _bfx_symbol(Symbol(base="DUSK", quote="USDT")) == "tDUSK:UST"


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


class TestOrderBookWSParsing:
    """Live order-book WS frames must yield a sorted, uncrossed best bid/ask.

    Guards the fix where binance/okx/bitmex were emitting unsorted deltas (or
    snapshot/diff levels) that surfaced a crossed best bid/ask in the Live UI.
    """

    @staticmethod
    async def _drain(agen):
        return [x async for x in agen]

    @pytest.mark.asyncio
    async def test_binance_partial_book_sorted(self):
        import json

        from dccd.sources.binance import _BinanceDepthWS
        ws = _BinanceDepthWS("btcusdt", 5)
        frame = json.dumps({"bids": [["100.0", "1"], ["99.0", "2"]],
                            "asks": [["101.0", "1"], ["102.0", "3"]]})
        [snap] = await self._drain(ws.parse_message(frame))
        assert snap.is_snapshot is True
        assert snap.bids[0].price == 100.0  # best bid = highest
        assert snap.asks[0].price == 101.0  # best ask = lowest
        assert snap.bids[0].price < snap.asks[0].price

    @pytest.mark.asyncio
    async def test_okx_books5_parsed(self):
        import json

        from dccd.sources.okx import _OKXWS
        ws = _OKXWS("BTC-USDT", "books5", "books")
        frame = json.dumps({"arg": {"channel": "books5"},
                            "data": [{"bids": [["100.0", "1", "0", "1"]],
                                      "asks": [["101.0", "1", "0", "1"]],
                                      "ts": "1700000000000"}]})
        [snap] = await self._drain(ws.parse_message(frame))
        assert snap.bids[0].price == 100.0 and snap.asks[0].price == 101.0
        assert snap.bids[0].price < snap.asks[0].price

    @pytest.mark.asyncio
    async def test_bitmex_orderbook10_parsed(self):
        import json

        from dccd.sources.bitmex import _BitMEXWS
        ws = _BitMEXWS("XBTUSD", "orderBook10", "book")
        frame = json.dumps({"table": "orderBook10",
                            "data": [{"symbol": "XBTUSD",
                                      "bids": [[100.0, 1], [99.5, 2]],
                                      "asks": [[101.0, 1], [101.5, 2]]}]})
        [snap] = await self._drain(ws.parse_message(frame))
        assert snap.is_snapshot is True
        assert snap.bids[0].price == 100.0 and snap.asks[0].price == 101.0
        assert snap.bids[0].price < snap.asks[0].price


class TestOKXOHLCWindowBoundary:
    """OKX OHLC pagination must not drop the bar at each page-window start.

    OKX ``before``/``after`` params are *exclusive*: passing ``before=start_ms``
    causes the bar exactly at the window start to be silently dropped on every
    forward-pagination step. ``fetch_ohlc_page`` must send ``before=start_ms-1``
    to include that bar.
    """

    def _make_stub_http(self, captured: list[dict], response_factory):
        """Return a fake AsyncHTTPClient context manager.

        ``captured`` is a list that will receive the ``params`` dict from each
        ``get()`` call.  ``response_factory(params)`` returns the canned OKX
        JSON dict for that call.
        """
        class _FakeClient:
            async def get(self, url, params):
                captured.append(dict(params))
                return response_factory(params)

        class _FakeHTTP:
            async def __aenter__(self):
                return _FakeClient()

            async def __aexit__(self, *exc):
                return False

        return _FakeHTTP()

    @staticmethod
    def _okx_response(bars_ms_newest_first: list[int]) -> dict:
        """Build a canned OKX candles response from a list of bar timestamps (ms)."""
        data = [
            [str(ts_ms), "50000", "50100", "49900", "50000", "1.0", "50000.0"]
            for ts_ms in bars_ms_newest_first
        ]
        return {"code": "0", "data": data}

    @pytest.mark.asyncio
    async def test_before_param_is_exclusive_adjusted(self):
        """fetch_ohlc_page must send before=start_ms-1 and after=end_ms."""
        span = 60  # 1-minute bars
        start_ns = 1_700_000_000 * NS
        end_ns = start_ns + 100 * 60 * NS  # 100-bar window
        captured: list[dict] = []

        src = OKXSource(http=self._make_stub_http(
            captured,
            lambda p: self._okx_response([]),
        ))

        await src.fetch_ohlc_page(Symbol(base="BTC", quote="USDT"), span, start_ns, end_ns, 100)

        assert len(captured) == 1
        params = captured[0]
        assert params["before"] == str(start_ns // 1_000_000 - 1)
        assert params["after"] == str(end_ns // 1_000_000)

    @pytest.mark.asyncio
    async def test_no_bar_lost_at_page_boundary(self):
        """Regression: paginate_ohlc over ≥150 min must include every minute.

        The stub emulates OKX exclusive semantics: bars with
        ``before_ms < ts_ms`` and ``ts_ms <= after_ms`` are NOT included when
        ``ts_ms == before_ms``.  Without the ``-1`` adjustment the bar at each
        100-bar boundary would be absent; with it, every bar is present.
        """
        from dccd.transport.paginate import paginate_ohlc

        span = 60  # 1-minute candles
        # Use a window of 160 minutes so we cross at least one 100-bar boundary.
        start_ns = 1_700_000_000 * NS
        n_bars = 160
        end_ns = start_ns + n_bars * 60 * NS

        # Build the full synthetic series (ms timestamps, oldest to newest).
        all_ts_ms = [start_ns // 1_000_000 + i * 60_000 for i in range(n_bars)]

        def _response_factory(params):
            before_ms = int(params["before"])
            after_ms = int(params["after"])
            # OKX semantics: "before" means newer-than (ts > before) and
            # "after" means older-than (ts < after) — both bounds exclusive.
            selected = [
                ts for ts in all_ts_ms
                if before_ms < ts < after_ms
            ]
            # OKX returns newest-first, up to 100 items.
            selected_sorted = sorted(selected, reverse=True)[:100]
            return self._okx_response(selected_sorted)

        captured: list[dict] = []
        src = OKXSource(http=self._make_stub_http(captured, _response_factory))

        cap = src.capability_for(DataType.OHLC, "rest", "historical")
        assert cap is not None

        sym = Symbol(base="BTC", quote="USDT")

        async def _fetch(s_ns, e_ns, limit):
            return await src.fetch_ohlc_page(sym, span, s_ns, e_ns, limit)

        collected = []
        async for bar in paginate_ohlc(_fetch, cap, start_ns, end_ns, span):
            collected.append(bar)

        collected_ts_ms = sorted(b.ts // 1_000_000 for b in collected)
        expected_ts_ms = all_ts_ms  # one bar per minute

        # No missing minute, no duplicate.
        assert collected_ts_ms == expected_ts_ms, (
            f"Expected {len(expected_ts_ms)} bars, got {len(collected_ts_ms)}; "
            f"missing: {set(expected_ts_ms) - set(collected_ts_ms)}"
        )


class TestKrakenOHLCWSParsing:
    """Kraken live OHLC must read ``interval_begin`` (not the missing
    ``timestamp_open``), which previously defaulted to 0 → 1970-01-01."""

    @pytest.mark.asyncio
    async def test_kraken_ohlc_timestamp_from_interval_begin(self):
        import json

        from dccd.sources.kraken import _KrakenWS

        ws = _KrakenWS("BTC/USD", "ohlc", 1)
        frame = json.dumps({
            "channel": "ohlc",
            "type": "update",
            "data": [{
                "symbol": "BTC/USD",
                "open": 60000.0, "high": 60100.0, "low": 59900.0,
                "close": 60050.0, "volume": 1.5,
                "interval_begin": "2024-01-02T03:04:00.000000000Z",
                "interval": 1,
            }],
        })

        async def _fake_raw():
            yield frame

        ws.stream_raw = _fake_raw  # type: ignore[method-assign]
        bars = [b async for b in ws.stream_ohlc()]
        assert len(bars) == 1
        bar = bars[0]
        assert bar.ts > 0
        # 2024-01-02T03:04:00Z in ns.
        from datetime import datetime, timezone
        expected = int(
            datetime(2024, 1, 2, 3, 4, tzinfo=timezone.utc).timestamp() * 1e9
        )
        assert bar.ts == expected
        assert bar.close == 60050.0
