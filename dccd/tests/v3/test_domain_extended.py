"""Extended domain tests — aggregate_ohlc multi-window, Symbol underscore, config validation."""

import pytest

from dccd.application.config import AppConfig, JobConfig
from dccd.domain.records import Trade
from dccd.domain.symbol import Symbol
from dccd.domain.timeutils import NS, s_to_ns
from dccd.domain.transforms import aggregate_ohlc

# ---------------------------------------------------------------------------
# aggregate_ohlc — multi-window coverage
# ---------------------------------------------------------------------------

class TestAggregateOHLCMultiWindow:
    def test_non_contiguous_windows(self) -> None:
        """Trades in windows 0, 2, 5 → exactly 3 bars, no phantom bars."""
        span = 60
        trades = [
            # window 0 (ts=0–59s)
            Trade(ts=s_to_ns(0), price=100.0, amount=1.0, side="buy"),
            Trade(ts=s_to_ns(30), price=105.0, amount=2.0, side="sell"),
            # window 2 (ts=120–179s) — gap at window 1
            Trade(ts=s_to_ns(120), price=200.0, amount=0.5, side="buy"),
            # window 5 (ts=300–359s) — gap at windows 3, 4
            Trade(ts=s_to_ns(300), price=300.0, amount=3.0, side="buy"),
            Trade(ts=s_to_ns(330), price=310.0, amount=1.0, side="sell"),
        ]
        bars = aggregate_ohlc(trades, span=span)

        assert len(bars) == 3, f"Expected 3 bars, got {len(bars)}: {[b.ts // NS for b in bars]}"

        # window 0
        b0 = bars[0]
        assert b0.open == 100.0
        assert b0.close == 105.0
        assert b0.high == 105.0
        assert b0.low == 100.0
        assert b0.volume == 3.0
        assert b0.trades == 2

        # window 2
        b2 = bars[1]
        assert b2.open == 200.0
        assert b2.volume == 0.5
        assert b2.trades == 1

        # window 5
        b5 = bars[2]
        assert b5.open == 300.0
        assert b5.close == 310.0
        assert b5.high == 310.0
        assert b5.volume == 4.0
        assert b5.trades == 2

    def test_aligned_to_span(self) -> None:
        """All bar timestamps must be aligned to span boundaries."""
        span = 3600
        trades = [
            Trade(ts=s_to_ns(1800), price=100.0, amount=1.0),   # mid-hour
            Trade(ts=s_to_ns(5400), price=110.0, amount=1.0),   # 1.5h
        ]
        bars = aggregate_ohlc(trades, span=span)
        span_ns = span * NS
        for bar in bars:
            assert bar.ts % span_ns == 0, f"Bar ts={bar.ts} not aligned to {span_ns}"

    def test_single_trade_per_window_ohlc_all_same(self) -> None:
        """Single trade per window → open == high == low == close."""
        trades = [Trade(ts=s_to_ns(0), price=42.0, amount=1.0)]
        bars = aggregate_ohlc(trades, span=60)
        assert len(bars) == 1
        b = bars[0]
        assert b.open == b.high == b.low == b.close == 42.0


# ---------------------------------------------------------------------------
# Symbol.parse — underscore separator
# ---------------------------------------------------------------------------

class TestSymbolParseSeparators:
    def test_underscore_separator(self) -> None:
        """BTC_USDT (Bybit WS format) should parse correctly."""
        s = Symbol.parse("BTC_USDT")
        assert s.base == "BTC"
        assert s.quote == "USDT"

    def test_slash_separator(self) -> None:
        s = Symbol.parse("ETH/USD")
        assert s.base == "ETH"

    def test_dash_separator(self) -> None:
        s = Symbol.parse("BTC-USD")
        assert s.base == "BTC"

    def test_xbt_normalised_with_dash(self) -> None:
        s = Symbol.parse("XBT-USD")
        assert s.base == "BTC"


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------

class TestJobConfigValidation:
    def test_valid_ohlc_job(self) -> None:
        jc = JobConfig(exchange="binance", pairs=["BTC/USDT"], data_type="ohlc", span=3600)
        assert jc.exchange == "binance"

    def test_unknown_exchange_raises(self) -> None:
        with pytest.raises(Exception, match="Unknown exchange"):
            JobConfig(exchange="ftx", pairs=["BTC/USDT"], data_type="ohlc", span=3600)

    def test_ohlc_without_span_raises(self) -> None:
        with pytest.raises(Exception, match="span.*required"):
            JobConfig(exchange="binance", pairs=["BTC/USDT"], data_type="ohlc")

    def test_trades_without_span_ok(self) -> None:
        jc = JobConfig(exchange="binance", pairs=["BTC/USDT"], data_type="trades")
        assert jc.span is None

    def test_empty_pairs_raises(self) -> None:
        with pytest.raises(Exception):
            JobConfig(exchange="binance", pairs=[], data_type="ohlc", span=3600)

    def test_pair_without_separator_raises(self) -> None:
        with pytest.raises(Exception):
            JobConfig(exchange="binance", pairs=["BTCUSDT"], data_type="ohlc", span=3600)


class TestAppConfigValidation:
    def test_empty_jobs_allowed(self) -> None:
        """AppConfig allows empty jobs — daemon can start and wait."""
        cfg = AppConfig()
        assert cfg.jobs == []

    def test_invalid_job_fails_config(self) -> None:
        with pytest.raises(Exception):
            AppConfig.model_validate({
                "jobs": [{"exchange": "ftx", "pairs": ["BTC/USDT"], "data_type": "ohlc", "span": 3600}]
            })


# ---------------------------------------------------------------------------
# Paginator window correctness
# ---------------------------------------------------------------------------

class TestOHLCPaginatorWindow:
    @pytest.mark.asyncio
    async def test_ohlc_window_equals_span_times_limit(self) -> None:
        """paginate_ohlc must size each window as span * max_per_request."""
        from dccd.domain.capability import Capability
        from dccd.domain.types import DataType
        from dccd.transport.paginate import paginate_ohlc

        span, max_per = 60, 1000
        cap = Capability(
            data_type=DataType.OHLC, transport="rest", mode="historical",
            history="full", max_per_request=max_per, page_direction="forward",
        )
        windows: list[tuple[int, int]] = []

        async def fetch(s_ns: int, e_ns: int, limit: int):
            windows.append((s_ns, e_ns))
            return []

        start = 0
        end = span * max_per * NS * 3  # exactly three full windows
        async for _ in paginate_ohlc(fetch, cap, start, end, span):
            pass

        assert windows, "paginator made no calls"
        first_s, first_e = windows[0]
        assert first_e - first_s == span * max_per * NS


class TestTradesCursorPaginator:
    @pytest.mark.asyncio
    async def test_follows_cursor_across_pages(self) -> None:
        """Cursor pagination drains every page, not just the first (the bug)."""
        from dccd.domain.capability import Capability
        from dccd.domain.types import DataType
        from dccd.transport.paginate import paginate_trades

        cap = Capability(
            data_type=DataType.TRADES, transport="rest", mode="historical",
            history="full", max_per_request=2, page_direction="forward",
        )

        def mk(ts_s: int) -> Trade:
            return Trade(ts=ts_s * NS, price=1.0, amount=1.0, side="buy", tid=str(ts_s))

        # Three pages of 2 trades each; cursor = next start second.
        pages = {
            None: ([mk(1), mk(2)], "3"),
            "3": ([mk(3), mk(4)], "5"),
            "5": ([mk(5)], None),
        }

        calls: list[str | None] = []

        async def fetch(s_ns, e_ns, limit, cursor):
            calls.append(cursor)
            return pages[cursor]

        out = [t async for t in paginate_trades(fetch, cap, 0, 100 * NS)]
        assert [int(t.tid) for t in out] == [1, 2, 3, 4, 5]
        assert calls == [None, "3", "5"]

    @pytest.mark.asyncio
    async def test_filters_outside_window_and_stops(self) -> None:
        """Items past end_ns are filtered and stop iteration."""
        from dccd.domain.capability import Capability
        from dccd.domain.types import DataType
        from dccd.transport.paginate import paginate_trades

        cap = Capability(
            data_type=DataType.TRADES, transport="rest", mode="historical",
            history="full", max_per_request=10, page_direction="forward",
        )

        def mk(ts_s: int) -> Trade:
            return Trade(ts=ts_s * NS, price=1.0, amount=1.0, side="buy", tid=str(ts_s))

        async def fetch(s_ns, e_ns, limit, cursor):
            return ([mk(1), mk(5), mk(20)], "next")

        out = [t async for t in paginate_trades(fetch, cap, 0, 10 * NS)]
        # ts=20s is past end (10s) → filtered, and iteration stops (no 2nd call).
        assert [int(t.tid) for t in out] == [1, 5]
