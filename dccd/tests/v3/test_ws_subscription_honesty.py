"""Honest WS subscriptions: declared depths + loud subscription rejections.

Production case: order-book jobs with ``depth: 20``/``depth: 50`` on Kraken
(WS v2 accepts only {10, 25, 100, 500, 1000}) were silently rejected — the
adapters filtered every non-data frame — leaving "live" streams that never
wrote anything.
"""

import asyncio
import json

import pytest

from dccd.application.jobs import JobParams, JobSpec, JobTarget, Trigger
from dccd.application.operations import stream
from dccd.domain.capability import Capability
from dccd.domain.records import OrderBookLevel, OrderBookSnapshot
from dccd.domain.symbol import Symbol
from dccd.domain.timeutils import ns_now
from dccd.domain.types import DataType
from dccd.sources.base import OrderBookLive
from dccd.sources.binance import BinanceSource
from dccd.sources.bitmex import BitMEXSource, _BitMEXWS
from dccd.sources.bybit import BybitSource, _BybitWS
from dccd.sources.kraken import KrakenSource, _KrakenWS
from dccd.sources.okx import _OKXWS, OKXSource
from dccd.storage.parquet import ParquetStore


class TestDeclaredDepths:
    """Every adapter with a live order-book capability declares its depths."""

    @pytest.mark.parametrize("source_cls", [
        KrakenSource, BybitSource, BinanceSource, OKXSource, BitMEXSource,
    ])
    def test_live_orderbook_declares_depths(self, source_cls):
        cap = source_cls().capability_for(DataType.ORDERBOOK, "ws", "live")
        assert cap is not None
        assert cap.depths, f"{source_cls.__name__} must declare valid depths"
        assert all(isinstance(d, int) and d > 0 for d in cap.depths)

    def test_kraken_depths_match_live_api(self):
        # Verified against the live API on 2026-06-10: 20 and 50 are rejected
        # with "Subscription depth not supported".
        cap = KrakenSource().capability_for(DataType.ORDERBOOK, "ws", "live")
        assert cap.depths == [10, 25, 100, 500, 1000]


class _SnapFake(OrderBookLive):
    """Fake adapter recording the depth the stream operation asks for."""

    exchange = "fake"

    def __init__(self, depths):
        self._depths = depths
        self.seen_depth = None

    def capabilities(self):
        return [Capability(data_type=DataType.ORDERBOOK, transport="ws",
                           mode="live", depths=self._depths)]

    async def stream_orderbook(self, symbol, depth, *, min_interval=0.0):
        self.seen_depth = depth
        yield OrderBookSnapshot(
            ts=ns_now(),
            bids=[OrderBookLevel(price=1.0, amount=1.0)],
            asks=[OrderBookLevel(price=2.0, amount=1.0)],
        )


class _FakeReg:
    def __init__(self, src):
        self._s = src

    def get(self, ex):
        return self._s


def _spec(depth):
    target = JobTarget(exchange="fake", symbol=Symbol.parse("BTC/USDT"),
                       data_type=DataType.ORDERBOOK)
    return JobSpec(id="stream:fake:BTC/USDT:orderbook", operation="stream",
                   target=target, trigger=Trigger(kind="supervised"),
                   params=JobParams(depth=depth, snapshot_interval=1),
                   origin="config")


class TestDepthSnap:
    @pytest.mark.asyncio
    async def test_invalid_depth_snaps_up_with_warning(self, tmp_path):
        src = _SnapFake([10, 25])
        store = ParquetStore(str(tmp_path / "data"))
        logs = []

        class _Ev:
            def log(self, msg, level="info"):
                logs.append((level, msg))

            def status(self, state):
                pass

            def sample(self, ts, **kw):
                pass

        stop = asyncio.Event()
        stop.set()  # break right after the first yielded snapshot
        await stream(_spec(20), registry=_FakeReg(src), store=store,
                     events=_Ev(), stop_event=stop)
        assert src.seen_depth == 25
        assert any(lvl == "warning" and "depth=20" in msg and "25" in msg
                   for lvl, msg in logs)

    @pytest.mark.asyncio
    async def test_valid_depth_passes_through(self, tmp_path):
        src = _SnapFake([10, 25])
        store = ParquetStore(str(tmp_path / "data"))
        stop = asyncio.Event()
        stop.set()
        await stream(_spec(25), registry=_FakeReg(src), store=store,
                     stop_event=stop)
        assert src.seen_depth == 25

    @pytest.mark.asyncio
    async def test_oversized_depth_snaps_to_largest(self, tmp_path):
        src = _SnapFake([10, 25])
        store = ParquetStore(str(tmp_path / "data"))
        stop = asyncio.Event()
        stop.set()
        await stream(_spec(9999), registry=_FakeReg(src), store=store,
                     stop_event=stop)
        assert src.seen_depth == 25


class TestSubscriptionRejectionRaises:
    """A scripted rejection frame must raise, not be silently filtered."""

    def _scripted(self, ws, frames):
        async def fake_stream_raw():
            for f in frames:
                yield json.dumps(f)
        ws.stream_raw = fake_stream_raw
        return ws

    @pytest.mark.asyncio
    async def test_kraken_book_rejection(self):
        ws = self._scripted(
            _KrakenWS("BTC/USD", "book", 20),
            [{"method": "subscribe", "success": False,
              "error": "Subscription depth not supported"}],
        )
        with pytest.raises(RuntimeError, match="kraken.*depth not supported"):
            async for _ in ws.stream_orderbook():
                pass

    @pytest.mark.asyncio
    async def test_bybit_rejection(self):
        ws = self._scripted(
            _BybitWS("BTCUSDT", "orderbook", "30"),
            [{"op": "subscribe", "success": False,
              "ret_msg": "Invalid depth"}],
        )
        with pytest.raises(RuntimeError, match="bybit.*Invalid depth"):
            async for _ in ws.stream_orderbook():
                pass

    @pytest.mark.asyncio
    async def test_okx_rejection(self):
        ws = self._scripted(
            _OKXWS("BTC-USDT", "books5", "books"),
            [{"event": "error", "code": "60018", "msg": "channel not exist"}],
        )
        with pytest.raises(RuntimeError, match="okx.*channel not exist"):
            async for _ in ws.stream():
                pass

    @pytest.mark.asyncio
    async def test_okx_rejection_inside_throttle_window(self):
        # Two frames: a normal book frame then an error — the error lands in
        # the throttle window and must still raise.
        ws = self._scripted(
            _OKXWS("BTC-USDT", "books5", "books"),
            [{"arg": {}, "data": [{"bids": [["1", "1"]], "asks": [["2", "1"]],
                                   "ts": "1700000000000"}]},
             {"event": "error", "code": "60018", "msg": "channel not exist"}],
        )
        seen = 0
        with pytest.raises(RuntimeError, match="okx"):
            async for _ in ws.stream(min_interval=60.0):
                seen += 1
        assert seen == 1  # first frame emitted, error raised on the second

    @pytest.mark.asyncio
    async def test_bitmex_rejection(self):
        ws = self._scripted(
            _BitMEXWS("XBTUSD", "orderBook10", "book"),
            [{"status": 400, "error": "Unknown table"}],
        )
        with pytest.raises(RuntimeError, match="bitmex.*Unknown table"):
            async for _ in ws.stream():
                pass
