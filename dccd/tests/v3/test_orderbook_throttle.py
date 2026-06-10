"""Tests for upstream order-book snapshot throttling.

Verifies that the ``min_interval`` knob in ``stream_orderbook`` limits pydantic
object construction to frames that will actually be saved — the fix for the
~96 % daemon CPU burn caused by per-frame ``OrderBookLevel``/``OrderBookSnapshot``
construction in ``_KrakenWS.stream_orderbook`` and ``_BybitWS.stream_orderbook``.
"""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _kraken_snapshot_frame(bids: list, asks: list) -> str:
    """Build a Kraken WS v2 ``book`` snapshot frame."""
    return json.dumps({
        "channel": "book",
        "type": "snapshot",
        "data": [{
            "symbol": "BTC/USD",
            "type": "snapshot",
            "bids": [{"price": str(p), "qty": str(q)} for p, q in bids],
            "asks": [{"price": str(p), "qty": str(q)} for p, q in asks],
        }],
    })


def _kraken_delta_frame(bids: list, asks: list) -> str:
    """Build a Kraken WS v2 ``book`` delta frame."""
    return json.dumps({
        "channel": "book",
        "type": "update",
        "data": [{
            "symbol": "BTC/USD",
            "type": "update",
            "bids": [{"price": str(p), "qty": str(q)} for p, q in bids],
            "asks": [{"price": str(p), "qty": str(q)} for p, q in asks],
        }],
    })


def _bybit_snapshot_frame(bids: list, asks: list, ts_ms: int = 1_700_000_000_000) -> str:
    return json.dumps({
        "type": "snapshot",
        "data": {
            "b": [[str(p), str(q)] for p, q in bids],
            "a": [[str(p), str(q)] for p, q in asks],
            "ts": ts_ms,
        },
    })


def _bybit_delta_frame(bids: list, asks: list, ts_ms: int = 1_700_000_001_000) -> str:
    return json.dumps({
        "type": "delta",
        "data": {
            "b": [[str(p), str(q)] for p, q in bids],
            "a": [[str(p), str(q)] for p, q in asks],
            "ts": ts_ms,
        },
    })


async def _collect(agen) -> list:
    return [x async for x in agen]


# ---------------------------------------------------------------------------
# Kraken throttle tests
# ---------------------------------------------------------------------------

class TestKrakenThrottle:
    """_KrakenWS.stream_orderbook throttles construction to min_interval."""

    @pytest.mark.asyncio
    async def test_min_interval_zero_yields_every_frame(self):
        """min_interval=0.0 (legacy) yields a snapshot on every frame."""
        from dccd.sources.kraken import _KrakenWS

        ws = _KrakenWS("BTC/USD", "book", 10)

        # 1 snapshot + 3 delta frames = 4 frames
        frames = [
            _kraken_snapshot_frame([(100.0, 1.0), (99.0, 2.0)], [(101.0, 1.0)]),
            _kraken_delta_frame([(98.0, 0.5)], []),
            _kraken_delta_frame([], [(102.0, 0.3)]),
            _kraken_delta_frame([(100.0, 0)], []),  # removal
        ]

        async def _fake_raw():
            for f in frames:
                yield f

        with patch.object(ws, "stream_raw", side_effect=_fake_raw):
            snaps = await _collect(ws.stream_orderbook(min_interval=0.0))

        assert len(snaps) == 4

    @pytest.mark.asyncio
    async def test_min_interval_throttles_to_one_per_window(self):
        """With min_interval=10, exactly 1 snapshot is emitted per 10-s window."""
        from dccd.sources.kraken import _KrakenWS

        ws = _KrakenWS("BTC/USD", "book", 10)

        # 1 snapshot + 5 delta frames; monotonic advances: 0, 0.5, 1, 2, 5, 15
        # Only the first frame (t=0) and the last (t=15) should be emitted
        # with min_interval=10.
        mono_values = [0.0, 0.5, 1.0, 2.0, 5.0, 15.0]
        frames = [
            _kraken_snapshot_frame([(100.0, 1.0)], [(101.0, 1.0)]),
            _kraken_delta_frame([(99.0, 0.5)], []),
            _kraken_delta_frame([], [(102.0, 0.3)]),
            _kraken_delta_frame([(100.0, 0.8)], []),
            _kraken_delta_frame([(98.0, 1.0)], []),
            _kraken_delta_frame([(97.0, 2.0)], []),
        ]

        async def _fake_raw():
            for f in frames:
                yield f

        mono_iter = iter(mono_values)

        with patch.object(ws, "stream_raw", side_effect=_fake_raw), \
             patch("dccd.sources.kraken.time.monotonic", side_effect=lambda: next(mono_iter)):
            snaps = await _collect(ws.stream_orderbook(min_interval=10.0))

        assert len(snaps) == 2, f"Expected 2 snapshots, got {len(snaps)}"

    @pytest.mark.asyncio
    async def test_truncates_to_depth(self):
        """Emitted snapshots have at most `depth` levels per side."""
        from dccd.sources.kraken import _KrakenWS

        depth = 3
        ws = _KrakenWS("BTC/USD", "book", depth)

        # Snapshot with more levels than depth
        bids = [(100.0 - i, 1.0) for i in range(10)]  # 10 bid levels
        asks = [(101.0 + i, 1.0) for i in range(10)]  # 10 ask levels
        frames = [_kraken_snapshot_frame(bids, asks)]

        async def _fake_raw():
            for f in frames:
                yield f

        with patch.object(ws, "stream_raw", side_effect=_fake_raw):
            snaps = await _collect(ws.stream_orderbook(min_interval=0.0))

        assert len(snaps) == 1
        snap = snaps[0]
        assert len(snap.bids) <= depth
        assert len(snap.asks) <= depth

    @pytest.mark.asyncio
    async def test_delta_removal_reflected_in_next_snapshot(self):
        """A delta removing a level (qty=0) is absent from the next snapshot."""
        from dccd.sources.kraken import _KrakenWS

        ws = _KrakenWS("BTC/USD", "book", 10)

        frames = [
            _kraken_snapshot_frame([(100.0, 1.0), (99.0, 2.0)], [(101.0, 1.0)]),
            _kraken_delta_frame([(100.0, 0)], []),  # remove 100.0 bid
        ]

        async def _fake_raw():
            for f in frames:
                yield f

        with patch.object(ws, "stream_raw", side_effect=_fake_raw):
            snaps = await _collect(ws.stream_orderbook(min_interval=0.0))

        assert len(snaps) == 2
        # After removal the last snapshot must not contain price 100.0 on bids
        last = snaps[-1]
        assert not any(lvl.price == 100.0 for lvl in last.bids)
        assert any(lvl.price == 99.0 for lvl in last.bids)

    @pytest.mark.asyncio
    async def test_is_snapshot_always_true(self):
        """All emitted snapshots have is_snapshot=True (full state, not raw delta)."""
        from dccd.sources.kraken import _KrakenWS

        ws = _KrakenWS("BTC/USD", "book", 10)

        frames = [
            _kraken_snapshot_frame([(100.0, 1.0)], [(101.0, 1.0)]),
            _kraken_delta_frame([(99.0, 0.5)], []),
        ]

        async def _fake_raw():
            for f in frames:
                yield f

        with patch.object(ws, "stream_raw", side_effect=_fake_raw):
            snaps = await _collect(ws.stream_orderbook(min_interval=0.0))

        for snap in snaps:
            assert snap.is_snapshot is True

    @pytest.mark.asyncio
    async def test_no_pydantic_construction_for_skipped_frames(self):
        """Pydantic objects are NOT constructed for throttled (skipped) frames."""
        import dccd.domain.records as records_mod
        from dccd.sources.kraken import _KrakenWS

        ws = _KrakenWS("BTC/USD", "book", 10)

        # 4 frames but min_interval = 10 so only first frame passes (mono: 0,1,2,3)
        frames = [
            _kraken_snapshot_frame([(100.0, 1.0)], [(101.0, 1.0)]),
            _kraken_delta_frame([(99.0, 0.5)], []),
            _kraken_delta_frame([], [(102.0, 0.3)]),
            _kraken_delta_frame([(98.0, 1.0)], []),
        ]
        mono_values = [0.0, 1.0, 2.0, 3.0]

        construction_count = [0]
        original_init = records_mod.OrderBookLevel.__init__

        def counting_init(self, **kwargs):
            construction_count[0] += 1
            original_init(self, **kwargs)

        async def _fake_raw():
            for f in frames:
                yield f

        mono_iter = iter(mono_values)

        with patch.object(ws, "stream_raw", side_effect=_fake_raw), \
             patch("dccd.sources.kraken.time.monotonic", side_effect=lambda: next(mono_iter)), \
             patch.object(records_mod.OrderBookLevel, "__init__", counting_init):
            snaps = await _collect(ws.stream_orderbook(min_interval=10.0))

        # Only 1 snapshot was emitted (the first frame)
        assert len(snaps) == 1
        # Only 2 OrderBookLevel objects were constructed (1 bid + 1 ask for that 1 frame)
        assert construction_count[0] == 2, (
            f"Expected 2 OrderBookLevel constructions (1 bid + 1 ask), got {construction_count[0]}"
        )


# ---------------------------------------------------------------------------
# Bybit throttle tests
# ---------------------------------------------------------------------------

class TestBybitThrottle:
    """_BybitWS.stream_orderbook throttles construction to min_interval."""

    @pytest.mark.asyncio
    async def test_min_interval_zero_yields_every_frame(self):
        from dccd.sources.bybit import _BybitWS

        ws = _BybitWS("BTCUSDT", "orderbook", "10")

        frames = [
            _bybit_snapshot_frame([(100.0, 1.0), (99.0, 2.0)], [(101.0, 1.0)]),
            _bybit_delta_frame([(98.0, 0.5)], []),
            _bybit_delta_frame([], [(102.0, 0.3)]),
        ]

        async def _fake_raw():
            for f in frames:
                yield f

        with patch.object(ws, "stream_raw", side_effect=_fake_raw):
            snaps = await _collect(ws.stream_orderbook(depth=10, min_interval=0.0))

        assert len(snaps) == 3

    @pytest.mark.asyncio
    async def test_min_interval_throttles_to_one_per_window(self):
        from dccd.sources.bybit import _BybitWS

        ws = _BybitWS("BTCUSDT", "orderbook", "10")

        mono_values = [0.0, 0.5, 1.0, 12.0]
        frames = [
            _bybit_snapshot_frame([(100.0, 1.0)], [(101.0, 1.0)]),
            _bybit_delta_frame([(99.0, 0.5)], []),
            _bybit_delta_frame([], [(102.0, 0.3)]),
            _bybit_delta_frame([(98.0, 1.0)], []),
        ]

        async def _fake_raw():
            for f in frames:
                yield f

        mono_iter = iter(mono_values)

        with patch.object(ws, "stream_raw", side_effect=_fake_raw), \
             patch("dccd.sources.bybit.time.monotonic", side_effect=lambda: next(mono_iter)):
            snaps = await _collect(ws.stream_orderbook(depth=10, min_interval=10.0))

        assert len(snaps) == 2, f"Expected 2 snapshots, got {len(snaps)}"

    @pytest.mark.asyncio
    async def test_truncates_to_depth(self):
        from dccd.sources.bybit import _BybitWS

        depth = 2
        ws = _BybitWS("BTCUSDT", "orderbook", str(depth))

        bids = [(100.0 - i, 1.0) for i in range(8)]
        asks = [(101.0 + i, 1.0) for i in range(8)]
        frames = [_bybit_snapshot_frame(bids, asks)]

        async def _fake_raw():
            for f in frames:
                yield f

        with patch.object(ws, "stream_raw", side_effect=_fake_raw):
            snaps = await _collect(ws.stream_orderbook(depth=depth, min_interval=0.0))

        assert len(snaps) == 1
        assert len(snaps[0].bids) <= depth
        assert len(snaps[0].asks) <= depth

    @pytest.mark.asyncio
    async def test_delta_removal_reflected_in_next_snapshot(self):
        from dccd.sources.bybit import _BybitWS

        ws = _BybitWS("BTCUSDT", "orderbook", "10")

        frames = [
            _bybit_snapshot_frame([(100.0, 1.0), (99.0, 2.0)], [(101.0, 1.0)]),
            _bybit_delta_frame([(100.0, 0)], []),  # remove 100.0 bid
        ]

        async def _fake_raw():
            for f in frames:
                yield f

        with patch.object(ws, "stream_raw", side_effect=_fake_raw):
            snaps = await _collect(ws.stream_orderbook(depth=10, min_interval=0.0))

        assert len(snaps) == 2
        last = snaps[-1]
        assert not any(lvl.price == 100.0 for lvl in last.bids)
        assert any(lvl.price == 99.0 for lvl in last.bids)

    @pytest.mark.asyncio
    async def test_is_snapshot_always_true(self):
        from dccd.sources.bybit import _BybitWS

        ws = _BybitWS("BTCUSDT", "orderbook", "10")

        frames = [
            _bybit_snapshot_frame([(100.0, 1.0)], [(101.0, 1.0)]),
            _bybit_delta_frame([(99.0, 0.5)], []),
        ]

        async def _fake_raw():
            for f in frames:
                yield f

        with patch.object(ws, "stream_raw", side_effect=_fake_raw):
            snaps = await _collect(ws.stream_orderbook(depth=10, min_interval=0.0))

        for snap in snaps:
            assert snap.is_snapshot is True


# ---------------------------------------------------------------------------
# Protocol signature test
# ---------------------------------------------------------------------------

class TestProtocolSignature:
    """All OrderBookLive adapters accept min_interval kwarg."""

    @pytest.mark.parametrize("adapter_class,args", [
        ("dccd.sources.kraken.KrakenSource", ()),
        ("dccd.sources.bybit.BybitSource", ()),
        ("dccd.sources.binance.BinanceSource", ()),
        ("dccd.sources.okx.OKXSource", ()),
        ("dccd.sources.bitmex.BitMEXSource", ()),
        ("dccd.sources.coinbase.CoinbaseSource", ()),
        ("dccd.sources.bitfinex.BitfinexSource", ()),
    ])
    def test_accepts_min_interval_kwarg(self, adapter_class, args):
        """stream_orderbook must accept min_interval as a keyword argument."""
        import importlib
        mod_path, cls_name = adapter_class.rsplit(".", 1)
        mod = importlib.import_module(mod_path)
        cls = getattr(mod, cls_name)
        import inspect
        sig = inspect.signature(cls().stream_orderbook)
        assert "min_interval" in sig.parameters, (
            f"{adapter_class}.stream_orderbook missing min_interval parameter"
        )
        param = sig.parameters["min_interval"]
        assert param.default == 0.0, (
            f"{adapter_class}.stream_orderbook: min_interval default should be 0.0"
        )
        assert param.kind == inspect.Parameter.KEYWORD_ONLY, (
            f"{adapter_class}.stream_orderbook: min_interval must be keyword-only"
        )
