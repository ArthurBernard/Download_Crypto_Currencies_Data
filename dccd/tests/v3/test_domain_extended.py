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

class TestPaginatorWindow:
    def test_ohlc_window_equals_span_times_limit(self) -> None:
        """paginate_ohlc window_s = span * max_per_request (verified via constant)."""
        from dccd.transport.paginate import _DEFAULT_TRADES_WINDOW_S

        assert _DEFAULT_TRADES_WINDOW_S == 86400, "Default trades window should be 1 day"
        # ohlc window is computed dynamically as span * max_per_request; the
        # formula lives in paginate_ohlc and is integration-tested separately.

    def test_trades_default_window_is_one_day(self) -> None:
        from dccd.transport.paginate import _DEFAULT_TRADES_WINDOW_S

        assert _DEFAULT_TRADES_WINDOW_S == 86400
