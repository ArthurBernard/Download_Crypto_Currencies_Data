"""Tests for source adapters — contract tests with mocked HTTP."""

from unittest.mock import AsyncMock, patch

import pytest

from dccd.domain.errors import NoCapability
from dccd.domain.symbol import Symbol
from dccd.domain.timeutils import NS
from dccd.domain.types import DataType
from dccd.sources.base import (
    FundingHistory,
    OHLCHistory,
    OHLCLive,
    OpenInterestHistory,
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
        assert self.src.render_symbol(BTC_USD) == "XBTUSD"

    def test_render_symbol_crypto_quote_btc(self):
        # ETH/BTC: BTC→XBT via _KRAKEN_ALIASES on the quote side → ETHXBT.
        assert self.src.render_symbol(Symbol(base="ETH", quote="BTC")) == "ETHXBT"

    def test_ohlc_cap_history_recent(self):
        cap = self.src.capability_for(DataType.OHLC, "rest", "historical")
        assert cap is not None
        assert cap.history == "recent"
        assert cap.max_per_request == 720

    def test_trades_cap_history_full(self):
        cap = self.src.capability_for(DataType.TRADES, "rest", "historical")
        assert cap is not None
        assert cap.history == "full"


class TestKrakenPairMapping:
    """Unit tests for _kraken_pair altname construction."""

    def test_legacy_btc_usd(self):
        from dccd.sources.kraken import _kraken_pair
        assert _kraken_pair(Symbol(base="BTC", quote="USD")) == "XBTUSD"

    def test_legacy_eth_btc(self):
        from dccd.sources.kraken import _kraken_pair
        assert _kraken_pair(Symbol(base="ETH", quote="BTC")) == "ETHXBT"

    def test_legacy_xrp_usd(self):
        from dccd.sources.kraken import _kraken_pair
        assert _kraken_pair(Symbol(base="XRP", quote="USD")) == "XRPUSD"

    def test_legacy_xrp_btc(self):
        from dccd.sources.kraken import _kraken_pair
        assert _kraken_pair(Symbol(base="XRP", quote="BTC")) == "XRPXBT"

    def test_modern_trx_usd(self):
        from dccd.sources.kraken import _kraken_pair
        assert _kraken_pair(Symbol(base="TRX", quote="USD")) == "TRXUSD"

    def test_modern_dot_btc(self):
        from dccd.sources.kraken import _kraken_pair
        assert _kraken_pair(Symbol(base="DOT", quote="BTC")) == "DOTXBT"

    def test_modern_bnb_usd(self):
        from dccd.sources.kraken import _kraken_pair
        assert _kraken_pair(Symbol(base="BNB", quote="USD")) == "BNBUSD"

    def test_doge_usd(self):
        from dccd.sources.kraken import _kraken_pair
        assert _kraken_pair(Symbol(base="DOGE", quote="USD")) == "XDGUSD"

    def test_doge_btc(self):
        from dccd.sources.kraken import _kraken_pair
        assert _kraken_pair(Symbol(base="DOGE", quote="BTC")) == "XDGXBT"


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


class TestBybitFundingHistory:
    """``linear`` ``perp`` funding — paired time params, newest-first backward paging."""

    def _make_stub_http(self, captured: list[tuple[str, dict]], responses):
        """Return a fake ``AsyncHTTPClient`` returning *responses* in sequence.

        *responses* may be a single dict (returned on every call) or a list of
        dicts consumed in order (the last entry repeats once exhausted).
        """
        seq = responses if isinstance(responses, list) else None

        class _FakeClient:
            def __init__(self) -> None:
                self._calls = 0

            async def get(self, url, params):
                captured.append((url, dict(params)))
                if seq is not None:
                    resp = seq[min(self._calls, len(seq) - 1)]
                    self._calls += 1
                    return resp
                return responses

        client = _FakeClient()

        class _FakeHTTP:
            async def __aenter__(self):
                return client

            async def __aexit__(self, *exc):
                return False

        return _FakeHTTP()

    def test_implements_funding_history(self):
        assert isinstance(BybitSource(), FundingHistory)

    def test_funding_cap_declares_perp_market_and_backward_paging(self):
        src = BybitSource()
        cap = src.capability_for(DataType.FUNDING, "rest", "historical")
        assert cap is not None
        assert cap.markets == ["perp"]
        assert cap.max_per_request == 200
        assert cap.page_direction == "backward"

    @pytest.mark.asyncio
    async def test_first_call_sends_both_start_and_end_time(self):
        captured: list[tuple[str, dict]] = []
        response = {"retCode": 0, "result": {"list": [
            {"fundingRate": "0.0001", "fundingRateTimestamp": "1600003600000", "symbol": "BTCUSDT"},
        ]}}
        src = BybitSource(http=self._make_stub_http(captured, response))

        rates, next_cursor = await src.fetch_funding_page(BTC_USDT, START_NS, END_NS, 200)

        assert len(captured) == 1
        url, params = captured[0]
        assert url == "https://api.bybit.com/v5/market/funding/history"
        assert params["category"] == "linear"
        assert params["symbol"] == "BTCUSDT"
        assert params["startTime"] == START_NS // 1_000_000
        assert params["endTime"] == END_NS // 1_000_000
        assert len(rates) == 1
        assert rates[0].ts == 1_600_003_600_000 * 1_000_000
        assert rates[0].rate == 0.0001
        assert next_cursor is None  # short page (1 item < limit=200)

    @pytest.mark.asyncio
    async def test_newest_first_two_page_backward_walk(self):
        captured: list[tuple[str, dict]] = []
        # Page 1: full page (limit=2), newest-first; oldest item ts=1_600_000_100_000 ms.
        page1 = {"retCode": 0, "result": {"list": [
            {"fundingRate": "0.0002", "fundingRateTimestamp": "1600000200000", "symbol": "BTCUSDT"},
            {"fundingRate": "0.0001", "fundingRateTimestamp": "1600000100000", "symbol": "BTCUSDT"},
        ]}}
        # Page 2: short page (1 item < limit=2) -> terminates the walk.
        page2 = {"retCode": 0, "result": {"list": [
            {"fundingRate": "0.0003", "fundingRateTimestamp": "1600000000000", "symbol": "BTCUSDT"},
        ]}}
        src = BybitSource(http=self._make_stub_http(captured, [page1, page2]))

        rates1, cursor1 = await src.fetch_funding_page(BTC_USDT, START_NS, END_NS, 2)
        assert len(rates1) == 2
        assert cursor1 == str(1_600_000_100_000 - 1)

        rates2, cursor2 = await src.fetch_funding_page(BTC_USDT, START_NS, END_NS, 2, cursor=cursor1)
        assert len(rates2) == 1
        assert cursor2 is None

        # cursor is reused as endTime on the follow-up call; startTime stays pinned.
        assert captured[1][1]["endTime"] == int(cursor1)
        assert captured[1][1]["startTime"] == START_NS // 1_000_000

    @pytest.mark.asyncio
    async def test_ret_code_error_returns_empty_no_crash(self):
        captured: list[tuple[str, dict]] = []
        response = {"retCode": 10001, "retMsg": "params error"}
        src = BybitSource(http=self._make_stub_http(captured, response))

        rates, next_cursor = await src.fetch_funding_page(BTC_USDT, START_NS, END_NS, 200)

        assert rates == []
        assert next_cursor is None

    @pytest.mark.asyncio
    async def test_limit_clamped_to_200(self):
        captured: list[tuple[str, dict]] = []
        response = {"retCode": 0, "result": {"list": []}}
        src = BybitSource(http=self._make_stub_http(captured, response))

        await src.fetch_funding_page(BTC_USDT, START_NS, END_NS, 5000)

        assert captured[0][1]["limit"] == 200


class TestBybitOpenInterestHistory:
    """``linear`` ``perp`` open interest — span-typed, real ``nextPageCursor``."""

    def _make_stub_http(self, captured: list[tuple[str, dict]], responses):
        """Return a fake ``AsyncHTTPClient`` returning *responses* in sequence.

        *responses* may be a single dict (returned on every call) or a list of
        dicts consumed in order (the last entry repeats once exhausted).
        """
        seq = responses if isinstance(responses, list) else None

        class _FakeClient:
            def __init__(self) -> None:
                self._calls = 0

            async def get(self, url, params):
                captured.append((url, dict(params)))
                if seq is not None:
                    resp = seq[min(self._calls, len(seq) - 1)]
                    self._calls += 1
                    return resp
                return responses

        client = _FakeClient()

        class _FakeHTTP:
            async def __aenter__(self):
                return client

            async def __aexit__(self, *exc):
                return False

        return _FakeHTTP()

    def test_implements_open_interest_history(self):
        assert isinstance(BybitSource(), OpenInterestHistory)

    def test_oi_cap_declares_perp_market_full_history_and_spans(self):
        src = BybitSource()
        cap = src.capability_for(DataType.OPEN_INTEREST, "rest", "historical")
        assert cap is not None
        assert cap.markets == ["perp"]
        assert cap.history == "full"
        assert cap.max_per_request == 200
        assert cap.page_direction == "backward"
        assert cap.spans == [300, 900, 1800, 3600, 14400, 86400]

    @pytest.mark.asyncio
    async def test_request_params(self):
        captured: list[tuple[str, dict]] = []
        response = {"retCode": 0, "result": {"list": [], "nextPageCursor": ""}}
        src = BybitSource(http=self._make_stub_http(captured, response))

        await src.fetch_oi_page(BTC_USDT, 3600, START_NS, END_NS, 200)

        assert len(captured) == 1
        url, params = captured[0]
        assert url == "https://api.bybit.com/v5/market/open-interest"
        assert params["category"] == "linear"
        assert params["symbol"] == "BTCUSDT"
        assert params["intervalTime"] == "1h"
        assert params["startTime"] == START_NS // 1_000_000
        assert params["endTime"] == END_NS // 1_000_000
        assert params["limit"] == 200
        assert "cursor" not in params

    @pytest.mark.asyncio
    async def test_limit_clamped_to_200(self):
        captured: list[tuple[str, dict]] = []
        response = {"retCode": 0, "result": {"list": []}}
        src = BybitSource(http=self._make_stub_http(captured, response))

        await src.fetch_oi_page(BTC_USDT, 3600, START_NS, END_NS, 5000)

        assert captured[0][1]["limit"] == 200

    @pytest.mark.asyncio
    async def test_cursor_sent_only_when_set(self):
        captured: list[tuple[str, dict]] = []
        response = {"retCode": 0, "result": {"list": []}}
        src = BybitSource(http=self._make_stub_http(captured, response))

        await src.fetch_oi_page(BTC_USDT, 3600, START_NS, END_NS, 200, cursor="abc123")

        assert captured[0][1]["cursor"] == "abc123"

    @pytest.mark.asyncio
    async def test_next_page_cursor_passthrough_when_present(self):
        captured: list[tuple[str, dict]] = []
        response = {"retCode": 0, "result": {"list": [], "nextPageCursor": "next-token"}}
        src = BybitSource(http=self._make_stub_http(captured, response))

        _, next_cursor = await src.fetch_oi_page(BTC_USDT, 3600, START_NS, END_NS, 200)

        assert next_cursor == "next-token"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("result", [{"list": []}, {"list": [], "nextPageCursor": ""}])
    async def test_next_page_cursor_none_when_empty_or_missing(self, result):
        captured: list[tuple[str, dict]] = []
        response = {"retCode": 0, "result": result}
        src = BybitSource(http=self._make_stub_http(captured, response))

        _, next_cursor = await src.fetch_oi_page(BTC_USDT, 3600, START_NS, END_NS, 200)

        assert next_cursor is None

    @pytest.mark.asyncio
    async def test_newest_first_parsing(self):
        captured: list[tuple[str, dict]] = []
        response = {"retCode": 0, "result": {"list": [
            {"openInterest": "5100.0", "timestamp": "1698768000000"},
            {"openInterest": "5000.0", "timestamp": "1698764400000"},
        ]}}
        src = BybitSource(http=self._make_stub_http(captured, response))

        oi, _ = await src.fetch_oi_page(BTC_USDT, 3600, START_NS, END_NS, 200)

        assert len(oi) == 2
        assert oi[0].ts == 1_698_768_000_000 * 1_000_000
        assert oi[0].open_interest == 5100.0
        assert oi[0].open_interest_value is None
        assert oi[1].ts == 1_698_764_400_000 * 1_000_000

    @pytest.mark.asyncio
    async def test_ret_code_error_returns_empty_no_crash(self):
        captured: list[tuple[str, dict]] = []
        response = {"retCode": 10001, "retMsg": "params error"}
        src = BybitSource(http=self._make_stub_http(captured, response))

        oi, next_cursor = await src.fetch_oi_page(BTC_USDT, 3600, START_NS, END_NS, 200)

        assert oi == []
        assert next_cursor is None

    @pytest.mark.asyncio
    async def test_unsupported_span_returns_empty_no_request(self):
        captured: list[tuple[str, dict]] = []
        response = {"retCode": 0, "result": {"list": []}}
        src = BybitSource(http=self._make_stub_http(captured, response))

        oi, next_cursor = await src.fetch_oi_page(BTC_USDT, 60, START_NS, END_NS, 200)

        assert oi == []
        assert next_cursor is None
        assert captured == []


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


class TestBinanceFuturesOHLCRouting:
    """Non-spot ``Symbol.market`` routes ``fetch_ohlc_page`` to the USDS-M
    futures ``continuousKlines`` endpoint instead of spot ``klines``."""

    def _make_stub_http(self, captured: list[tuple[str, dict]]):
        """Return a fake ``AsyncHTTPClient`` context manager.

        ``captured`` receives ``(url, params)`` tuples from each ``get()`` call.
        """
        class _FakeClient:
            async def get(self, url, params):
                captured.append((url, dict(params)))
                return [
                    [1_600_000_000_000, "50000", "51000", "49000", "50500",
                     "100", 1_600_003_600_000, "5000000", 500, "50", "2500000", "0"]
                ]

        class _FakeHTTP:
            async def __aenter__(self):
                return _FakeClient()

            async def __aexit__(self, *exc):
                return False

        return _FakeHTTP()

    @pytest.mark.asyncio
    async def test_spot_symbol_uses_spot_klines(self):
        captured: list[tuple[str, dict]] = []
        src = BinanceSource(http=self._make_stub_http(captured))

        await src.fetch_ohlc_page(BTC_USDT, 3600, START_NS, END_NS, 500)

        assert len(captured) == 1
        url, params = captured[0]
        assert url == "https://api.binance.com/api/v3/klines"
        assert params["symbol"] == "BTCUSDT"
        assert "pair" not in params
        assert "contractType" not in params
        assert params["limit"] == 500

    @pytest.mark.asyncio
    async def test_perp_symbol_uses_continuous_klines(self):
        captured: list[tuple[str, dict]] = []
        src = BinanceSource(http=self._make_stub_http(captured))
        sym = Symbol(base="BTC", quote="USDT", market="perp")

        await src.fetch_ohlc_page(sym, 3600, START_NS, END_NS, 500)

        assert len(captured) == 1
        url, params = captured[0]
        assert url == "https://fapi.binance.com/fapi/v1/continuousKlines"
        assert params["pair"] == "BTCUSDT"
        assert params["contractType"] == "PERPETUAL"
        assert "symbol" not in params

    @pytest.mark.asyncio
    async def test_quarter_symbol_uses_current_quarter(self):
        captured: list[tuple[str, dict]] = []
        src = BinanceSource(http=self._make_stub_http(captured))
        sym = Symbol(base="BTC", quote="USDT", market="quarter")

        await src.fetch_ohlc_page(sym, 86400, START_NS, END_NS, 500)

        assert len(captured) == 1
        _, params = captured[0]
        assert params["contractType"] == "CURRENT_QUARTER"

    @pytest.mark.asyncio
    async def test_next_quarter_symbol_uses_next_quarter(self):
        captured: list[tuple[str, dict]] = []
        src = BinanceSource(http=self._make_stub_http(captured))
        sym = Symbol(base="BTC", quote="USDT", market="next_quarter")

        await src.fetch_ohlc_page(sym, 86400, START_NS, END_NS, 500)

        assert len(captured) == 1
        _, params = captured[0]
        assert params["contractType"] == "NEXT_QUARTER"

    @pytest.mark.asyncio
    async def test_futures_limit_clamped_to_1500(self):
        captured: list[tuple[str, dict]] = []
        src = BinanceSource(http=self._make_stub_http(captured))
        sym = Symbol(base="BTC", quote="USDT", market="perp")

        await src.fetch_ohlc_page(sym, 3600, START_NS, END_NS, 5000)

        assert len(captured) == 1
        _, params = captured[0]
        assert params["limit"] == 1500

    def test_ohlc_rest_capability_declares_futures_markets(self):
        src = BinanceSource()
        cap = src.capability_for(DataType.OHLC, "rest", "historical")
        assert cap is not None
        assert cap.markets == ["spot", "perp", "quarter", "next_quarter"]
        assert cap.max_per_request == 1000

    def test_ws_capabilities_declare_no_futures_markets(self):
        src = BinanceSource()
        ws_caps = [c for c in src.capabilities() if c.transport == "ws"]
        assert ws_caps, "expected at least one WS capability"
        assert all(c.markets is None for c in ws_caps)


class TestBinanceFundingHistory:
    """USDS-M ``fundingRate`` — realized funding, ``perp`` market only."""

    def _make_stub_http(self, captured: list[tuple[str, dict]], response):
        """Return a fake ``AsyncHTTPClient`` context manager returning *response*."""
        class _FakeClient:
            async def get(self, url, params):
                captured.append((url, dict(params)))
                return response

        class _FakeHTTP:
            async def __aenter__(self):
                return _FakeClient()

            async def __aexit__(self, *exc):
                return False

        return _FakeHTTP()

    def test_implements_funding_history(self):
        assert isinstance(BinanceSource(), FundingHistory)

    def test_funding_cap_declares_perp_market(self):
        src = BinanceSource()
        cap = src.capability_for(DataType.FUNDING, "rest", "historical")
        assert cap is not None
        assert cap.markets == ["perp"]
        assert cap.max_per_request == 1000

    @pytest.mark.asyncio
    async def test_first_call_uses_start_ns_as_start_time(self):
        captured: list[tuple[str, dict]] = []
        response = [
            {"fundingTime": 1_600_000_000_000, "fundingRate": "0.00010000", "markPrice": "50000.00"},
        ]
        src = BinanceSource(http=self._make_stub_http(captured, response))
        sym = Symbol(base="BTC", quote="USDT", market="perp")

        rates, next_cursor = await src.fetch_funding_page(sym, START_NS, END_NS, 1000)

        assert len(captured) == 1
        url, params = captured[0]
        assert url == "https://fapi.binance.com/fapi/v1/fundingRate"
        assert params["symbol"] == "BTCUSDT"
        assert params["startTime"] == START_NS // 1_000_000
        assert params["endTime"] == END_NS // 1_000_000
        assert len(rates) == 1
        assert rates[0].ts == 1_600_000_000_000 * 1_000_000
        assert rates[0].rate == 0.0001
        assert rates[0].mark_price == 50000.0
        assert next_cursor is None  # short page (1 item < limit=1000)

    @pytest.mark.asyncio
    async def test_followup_call_uses_cursor_as_start_time(self):
        captured: list[tuple[str, dict]] = []
        src = BinanceSource(http=self._make_stub_http(captured, []))
        sym = Symbol(base="BTC", quote="USDT", market="perp")

        await src.fetch_funding_page(sym, START_NS, END_NS, 1000, cursor="1600003600001")

        assert len(captured) == 1
        _, params = captured[0]
        assert params["startTime"] == 1600003600001

    @pytest.mark.asyncio
    async def test_full_page_returns_next_cursor(self):
        captured: list[tuple[str, dict]] = []
        response = [
            {"fundingTime": 1_600_000_000_000 + i * 28_800_000, "fundingRate": "0.0001", "markPrice": "50000"}
            for i in range(2)
        ]
        src = BinanceSource(http=self._make_stub_http(captured, response))
        sym = Symbol(base="BTC", quote="USDT", market="perp")

        rates, next_cursor = await src.fetch_funding_page(sym, START_NS, END_NS, 2)

        assert len(rates) == 2
        assert next_cursor == str(response[-1]["fundingTime"] + 1)

    @pytest.mark.asyncio
    async def test_empty_mark_price_becomes_none(self):
        captured: list[tuple[str, dict]] = []
        response = [
            {"fundingTime": 1_600_000_000_000, "fundingRate": "-0.0002", "markPrice": ""},
        ]
        src = BinanceSource(http=self._make_stub_http(captured, response))
        sym = Symbol(base="BTC", quote="USDT", market="perp")

        rates, _ = await src.fetch_funding_page(sym, START_NS, END_NS, 1000)

        assert rates[0].mark_price is None
        assert rates[0].rate == -0.0002

    @pytest.mark.asyncio
    async def test_limit_clamped_to_1000(self):
        captured: list[tuple[str, dict]] = []
        src = BinanceSource(http=self._make_stub_http(captured, []))
        sym = Symbol(base="BTC", quote="USDT", market="perp")

        await src.fetch_funding_page(sym, START_NS, END_NS, 5000)

        assert len(captured) == 1
        _, params = captured[0]
        assert params["limit"] == 1000
