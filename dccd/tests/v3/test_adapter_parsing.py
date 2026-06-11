"""Offline parsing tests for exchange adapters.

Each test loads a verbatim fixture captured from the live exchange API
(see dccd/tests/v3/fixtures/README.md for capture commands and dates)
and feeds it through the adapter's pure parse helper, asserting record
counts, ns-UTC int64 timestamps, field mapping, and side/symbol normalisation.

These tests are NOT network-marked and run entirely offline.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dccd.domain.records import OHLCBar, Trade
from dccd.domain.symbol import Symbol

# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

_FIXTURES = Path(__file__).parent / "fixtures"


def _load(name: str) -> object:
    return json.loads((_FIXTURES / name).read_text())


# ---------------------------------------------------------------------------
# Bitfinex
# ---------------------------------------------------------------------------

class TestBitfinexOHLCParsing:
    """Feed the bitfinex_ohlc_page.json fixture through _parse_ohlc_page."""

    def setup_method(self) -> None:
        from dccd.sources.bitfinex import _parse_ohlc_page
        self.parse = _parse_ohlc_page
        self.raw = _load("bitfinex_ohlc_page.json")

    def test_record_count(self) -> None:
        bars = self.parse(self.raw)
        assert len(bars) == 5

    def test_records_are_ohlc_bars(self) -> None:
        bars = self.parse(self.raw)
        assert all(isinstance(b, OHLCBar) for b in bars)

    def test_timestamps_are_ns_int64(self) -> None:
        bars = self.parse(self.raw)
        for b in bars:
            assert isinstance(b.ts, int)
            # ns are larger than ms by 1e6
            assert b.ts > 1_000_000_000_000_000_000  # > year 2001 in ns

    def test_first_bar_values(self) -> None:
        """Spot-check the first bar from the fixture."""
        bars = self.parse(self.raw)
        # fixture[0] = [1364770800000, 93.2, 93.033, 93.29, 92.9, 116.00...]
        # Bitfinex order: ts_ms, open, close, high, low, volume
        b = bars[0]
        assert b.ts == 1364770800000 * 1_000_000
        assert b.open == pytest.approx(93.2)
        assert b.close == pytest.approx(93.033)
        assert b.high == pytest.approx(93.29)
        assert b.low == pytest.approx(92.9)
        assert b.volume == pytest.approx(116.001802)

    def test_ascending_timestamps(self) -> None:
        bars = self.parse(self.raw)
        ts = [b.ts for b in bars]
        assert ts == sorted(ts)

    def test_empty_input(self) -> None:
        assert self.parse([]) == []

    def test_invalid_rows_skipped(self) -> None:
        # Rows that are too short or not lists are skipped silently
        data = [[1364770800000, 93.2, 93.033, 93.29, 92.9, 116.0], "bad", [1, 2]]
        bars = self.parse(data)
        assert len(bars) == 1


class TestBitfinexTradesParsing:
    """Feed the bitfinex_trades_page.json fixture through _parse_trades_page."""

    def setup_method(self) -> None:
        from dccd.sources.bitfinex import _parse_trades_page
        self.parse = _parse_trades_page
        self.raw = _load("bitfinex_trades_page.json")

    def test_record_count(self) -> None:
        trades, _ = self.parse(self.raw)
        assert len(trades) == 5

    def test_records_are_trades(self) -> None:
        trades, _ = self.parse(self.raw)
        assert all(isinstance(t, Trade) for t in trades)

    def test_timestamps_are_ns_int64(self) -> None:
        trades, _ = self.parse(self.raw)
        for t in trades:
            assert isinstance(t.ts, int)
            # Fixture ts_ms starts around 2013 — still > 1e18 ns
            assert t.ts > 1_300_000_000_000_000_000

    def test_side_mapping_positive_amount_is_buy(self) -> None:
        # fixture row [4145,1358182043000,0.2721858,14.5373664] — amount > 0 → buy
        trades, _ = self.parse(self.raw)
        assert trades[0].side == "buy"

    def test_amount_is_absolute(self) -> None:
        trades, _ = self.parse(self.raw)
        for t in trades:
            assert t.amount >= 0

    def test_tid_is_string(self) -> None:
        trades, _ = self.parse(self.raw)
        assert all(isinstance(t.tid, str) for t in trades)

    def test_cursor_not_returned_by_parse_helper(self) -> None:
        # The helper itself doesn't decide the cursor; the fetch method does.
        _, cursor = self.parse(self.raw)
        assert cursor is None

    def test_sell_side_from_negative_amount(self) -> None:
        # Inject a row with negative amount to test sell branch
        sell_row = [9999, 1358220000000, -2.5, 14.6]
        trades, _ = self.parse([sell_row])
        assert trades[0].side == "sell"
        assert trades[0].amount == pytest.approx(2.5)


class TestBitfinexWSParsing:
    """Feed the WS message fixtures through _BitfinexWS.parse_message."""

    def _make_ws(self, channel: str) -> object:
        from dccd.sources.bitfinex import _BitfinexWS
        return _BitfinexWS("tBTCUSD", channel)

    def _collect(self, ws: object, raw: str) -> list:
        """Drain parse_message into a list synchronously."""
        import asyncio

        async def _drain() -> list:
            records = []
            async for r in ws.parse_message(raw):
                records.append(r)
            return records

        return asyncio.get_event_loop().run_until_complete(_drain())

    def test_subscribe_ack_yields_nothing(self) -> None:
        ws = self._make_ws("trades")
        msgs = _load("bitfinex_ws_trade_msgs.json")
        ack = json.dumps(msgs[0])
        records = self._collect(ws, ack)
        assert records == []
        # chan_id is set after ack
        assert ws._chan_id == 17

    def test_heartbeat_yields_nothing(self) -> None:
        ws = self._make_ws("trades")
        ws._chan_id = 17
        hb = json.dumps([17, "hb"])
        assert self._collect(ws, hb) == []

    def test_trade_buy_parsed(self) -> None:
        ws = self._make_ws("trades")
        ws._chan_id = 17
        msgs = _load("bitfinex_ws_trade_msgs.json")
        raw = json.dumps(msgs[2])  # [17, [4161, 1358217143000, 44.0, 14.52...]]
        records = self._collect(ws, raw)
        assert len(records) == 1
        t = records[0]
        assert isinstance(t, Trade)
        assert t.side == "buy"  # amount 44.0 > 0
        assert t.ts == 1358217143000 * 1_000_000

    def test_trade_sell_parsed(self) -> None:
        ws = self._make_ws("trades")
        ws._chan_id = 17
        msgs = _load("bitfinex_ws_trade_msgs.json")
        raw = json.dumps(msgs[3])  # [17, [4162, 1358220743000, -5.5, 14.6]]
        records = self._collect(ws, raw)
        assert len(records) == 1
        assert records[0].side == "sell"
        assert records[0].amount == pytest.approx(5.5)

    def test_candle_snapshot_array_parsed(self) -> None:
        ws = self._make_ws("candles")
        ws._chan_id = 42
        msgs = _load("bitfinex_ws_candle_msgs.json")
        # msgs[1] = [42, [[bar1], [bar2]]] — snapshot with 2 bars
        raw = json.dumps(msgs[1])
        records = self._collect(ws, raw)
        assert len(records) == 2
        assert all(isinstance(r, OHLCBar) for r in records)

    def test_candle_single_update_parsed(self) -> None:
        ws = self._make_ws("candles")
        ws._chan_id = 42
        msgs = _load("bitfinex_ws_candle_msgs.json")
        # msgs[2] = [42, [ts, o, c, h, l, v]] — single bar
        raw = json.dumps(msgs[2])
        records = self._collect(ws, raw)
        assert len(records) == 1
        assert isinstance(records[0], OHLCBar)


# ---------------------------------------------------------------------------
# BitMEX
# ---------------------------------------------------------------------------

class TestBitMEXOHLCParsing:
    """Feed the bitmex_ohlc_page.json fixture through _parse_ohlc_page."""

    def setup_method(self) -> None:
        from dccd.sources.bitmex import _parse_ohlc_page
        self.parse = _parse_ohlc_page
        self.raw = _load("bitmex_ohlc_page.json")

    def test_record_count(self) -> None:
        bars = self.parse(self.raw)
        assert len(bars) == 5

    def test_records_are_ohlc_bars(self) -> None:
        bars = self.parse(self.raw)
        assert all(isinstance(b, OHLCBar) for b in bars)

    def test_timestamps_ns_utc(self) -> None:
        bars = self.parse(self.raw)
        for b in bars:
            assert isinstance(b.ts, int)
            # 2024-01-01 = 1704067200 s → 1704067200_000_000_000 ns
            assert b.ts >= 1_704_067_200_000_000_000

    def test_first_bar_values(self) -> None:
        """Fixture timestamp = 2024-01-01T00:00:00Z."""
        bars = self.parse(self.raw)
        b = bars[0]
        assert b.ts == 1_704_067_200_000_000_000
        assert b.open == pytest.approx(42282.0)
        assert b.high == pytest.approx(42374.0)
        assert b.low == pytest.approx(42083.5)
        assert b.close == pytest.approx(42312.0)

    def test_end_ns_filter(self) -> None:
        """Bars past end_ns must be dropped."""
        # Only keep the first bar (2024-01-01T00:00:00Z)
        end_ns = 1_704_067_200_000_000_000  # exactly the first bar
        bars = self.parse(self.raw, end_ns=end_ns)
        assert len(bars) == 1

    def test_invalid_timestamp_skipped(self) -> None:
        data = [{"timestamp": "NOT_A_DATE", "open": 1, "high": 2, "low": 0, "close": 1, "volume": 10}]
        bars = self.parse(data)
        assert bars == []


class TestBitMEXTradesParsing:
    """Feed the bitmex_trades_page.json fixture through _parse_trades_page."""

    def setup_method(self) -> None:
        from dccd.sources.bitmex import _parse_trades_page
        self.parse = _parse_trades_page
        self.raw = _load("bitmex_trades_page.json")

    def test_record_count(self) -> None:
        trades = self.parse(self.raw)
        assert len(trades) == 5

    def test_records_are_trades(self) -> None:
        trades = self.parse(self.raw)
        assert all(isinstance(t, Trade) for t in trades)

    def test_sell_side(self) -> None:
        trades = self.parse(self.raw)
        assert trades[0].side == "sell"

    def test_buy_side(self) -> None:
        """Last fixture row has side=Buy."""
        trades = self.parse(self.raw)
        assert trades[-1].side == "buy"

    def test_xbt_symbol_normalisation(self) -> None:
        """render_symbol maps BTC→XBT; Symbol normalisation BTC stays BTC."""
        from dccd.sources.bitmex import BitMEXSource
        src = BitMEXSource()
        assert src.render_symbol(Symbol(base="BTC", quote="USD")) == "XBTUSD"

    def test_tid_is_uuid_string(self) -> None:
        trades = self.parse(self.raw)
        assert trades[0].tid == "00000000-006d-1000-0000-0003ad666d43"

    def test_timestamps_ns_utc(self) -> None:
        trades = self.parse(self.raw)
        for t in trades:
            assert isinstance(t.ts, int)
            assert t.ts >= 1_704_067_200_000_000_000


class TestBitMEXWSParsing:
    """Feed bitmex_ws_trade_msgs.json through _BitMEXWS.parse_message."""

    def _collect(self, ws: object, raw: str) -> list:
        import asyncio

        async def _drain() -> list:
            records = []
            async for r in ws.parse_message(raw):
                records.append(r)
            return records

        return asyncio.get_event_loop().run_until_complete(_drain())

    def _make_ws(self, mode: str) -> object:
        from dccd.sources.bitmex import _BitMEXWS
        return _BitMEXWS("XBTUSD", "trade" if mode == "trades" else mode, mode)

    def test_welcome_message_yields_nothing(self) -> None:
        ws = self._make_ws("trades")
        msgs = _load("bitmex_ws_trade_msgs.json")
        records = self._collect(ws, json.dumps(msgs[0]))
        assert records == []

    def test_sub_ack_yields_nothing(self) -> None:
        ws = self._make_ws("trades")
        msgs = _load("bitmex_ws_trade_msgs.json")
        records = self._collect(ws, json.dumps(msgs[1]))
        assert records == []

    def test_insert_yields_two_trades(self) -> None:
        ws = self._make_ws("trades")
        msgs = _load("bitmex_ws_trade_msgs.json")
        records = self._collect(ws, json.dumps(msgs[2]))
        assert len(records) == 2
        assert all(isinstance(r, Trade) for r in records)

    def test_sell_and_buy_sides(self) -> None:
        ws = self._make_ws("trades")
        msgs = _load("bitmex_ws_trade_msgs.json")
        records = self._collect(ws, json.dumps(msgs[2]))
        assert records[0].side == "sell"
        assert records[1].side == "buy"

    def test_timestamps_ns_utc(self) -> None:
        ws = self._make_ws("trades")
        msgs = _load("bitmex_ws_trade_msgs.json")
        records = self._collect(ws, json.dumps(msgs[2]))
        for r in records:
            assert isinstance(r.ts, int)
            assert r.ts >= 1_704_067_200_000_000_000


# ---------------------------------------------------------------------------
# Coinbase
# ---------------------------------------------------------------------------

class TestCoinbaseOHLCParsing:
    """Feed coinbase_candles_page.json through _parse_ohlc_page."""

    def setup_method(self) -> None:
        from dccd.sources.coinbase import _parse_ohlc_page
        self.parse = _parse_ohlc_page
        self.raw = _load("coinbase_candles_page.json")

    def test_record_count(self) -> None:
        bars = self.parse(self.raw)
        assert len(bars) == 6

    def test_records_are_ohlc_bars(self) -> None:
        bars = self.parse(self.raw)
        assert all(isinstance(b, OHLCBar) for b in bars)

    def test_timestamps_ns_utc(self) -> None:
        bars = self.parse(self.raw)
        for b in bars:
            assert isinstance(b.ts, int)
            # 2024-01-01 = 1704067200s → 1704067200_000_000_000 ns
            assert b.ts >= 1_704_067_200_000_000_000

    def test_field_mapping(self) -> None:
        """Coinbase format: [time_s, low, high, open, close, volume]."""
        # Last element in fixture: [1704067200,42261.58,42543.64,42288.58,42452.66,379.197...]
        bars = self.parse(self.raw)
        b = bars[-1]
        assert b.ts == 1_704_067_200_000_000_000
        assert b.low == pytest.approx(42261.58)
        assert b.high == pytest.approx(42543.64)
        assert b.open == pytest.approx(42288.58)
        assert b.close == pytest.approx(42452.66)

    def test_quote_volume_is_none(self) -> None:
        """Coinbase carries no quote volume; field must be None."""
        bars = self.parse(self.raw)
        for b in bars:
            assert b.quote_volume is None

    def test_empty_input(self) -> None:
        assert self.parse([]) == []

    def test_short_rows_skipped(self) -> None:
        bars = self.parse([[1704067200, 42261.58, 42543.64]])  # only 3 elements
        assert bars == []


class TestCoinbaseTradesParsing:
    """Feed coinbase_trades_page.json through _parse_trades_page."""

    def setup_method(self) -> None:
        from dccd.sources.coinbase import _parse_trades_page
        self.parse = _parse_trades_page
        self.raw = _load("coinbase_trades_page.json")

    def test_record_count_unfiltered(self) -> None:
        trades = self.parse(self.raw)
        assert len(trades) == 5

    def test_records_are_trades(self) -> None:
        trades = self.parse(self.raw)
        assert all(isinstance(t, Trade) for t in trades)

    def test_side_mapping(self) -> None:
        trades = self.parse(self.raw)
        # All fixture entries have side="buy"
        assert all(t.side == "buy" for t in trades)

    def test_tid_is_string(self) -> None:
        trades = self.parse(self.raw)
        assert trades[0].tid == "1036397971"

    def test_timestamps_ns_utc(self) -> None:
        trades = self.parse(self.raw)
        for t in trades:
            assert isinstance(t.ts, int)
            # 2026-06-11T12:53Z ≈ 1781516018s → > 1.78e18 ns
            assert t.ts > 1_780_000_000_000_000_000

    def test_window_filter_drops_out_of_range(self) -> None:
        """Trades outside [start_ns, end_ns] are silently dropped."""
        # All fixture trades are ~2026-06-11; a window in 2024 yields nothing.
        start_ns = 1_704_067_200_000_000_000
        end_ns = 1_704_070_800_000_000_000
        trades = self.parse(self.raw, start_ns=start_ns, end_ns=end_ns)
        assert trades == []


class TestCoinbaseWSParsing:
    """Feed coinbase_ws_trade_msgs.json through _CoinbaseWS.parse_message."""

    def _make_ws(self) -> object:
        from dccd.sources.coinbase import _CoinbaseWS
        return _CoinbaseWS("BTC-USD")

    def _collect(self, ws: object, raw: str) -> list:
        import asyncio

        async def _drain() -> list:
            records = []
            async for r in ws.parse_message(raw):
                records.append(r)
            return records

        return asyncio.get_event_loop().run_until_complete(_drain())

    def test_snapshot_event_yields_two_trades(self) -> None:
        ws = self._make_ws()
        msgs = _load("coinbase_ws_trade_msgs.json")
        records = self._collect(ws, json.dumps(msgs[0]))
        assert len(records) == 2
        assert all(isinstance(r, Trade) for r in records)

    def test_update_event_yields_one_trade(self) -> None:
        ws = self._make_ws()
        msgs = _load("coinbase_ws_trade_msgs.json")
        records = self._collect(ws, json.dumps(msgs[1]))
        assert len(records) == 1

    def test_side_lowercased(self) -> None:
        ws = self._make_ws()
        msgs = _load("coinbase_ws_trade_msgs.json")
        # First snapshot has side="BUY" — should be lowercased to "buy"
        records = self._collect(ws, json.dumps(msgs[0]))
        assert records[0].side == "buy"

    def test_timestamps_ns_utc(self) -> None:
        ws = self._make_ws()
        msgs = _load("coinbase_ws_trade_msgs.json")
        records = self._collect(ws, json.dumps(msgs[0]))
        for r in records:
            assert isinstance(r.ts, int)
            assert r.ts > 1_780_000_000_000_000_000

    def test_empty_events_yields_nothing(self) -> None:
        ws = self._make_ws()
        records = self._collect(ws, json.dumps({"channel": "market_trades", "events": []}))
        assert records == []


# ---------------------------------------------------------------------------
# Binance
# ---------------------------------------------------------------------------

class TestBinanceOHLCParsing:
    """Feed binance_klines_page.json through _parse_ohlc_page."""

    def setup_method(self) -> None:
        from dccd.sources.binance import _parse_ohlc_page
        self.parse = _parse_ohlc_page
        self.raw = _load("binance_klines_page.json")

    def test_record_count(self) -> None:
        bars = self.parse(self.raw)
        assert len(bars) == 5

    def test_records_are_ohlc_bars(self) -> None:
        bars = self.parse(self.raw)
        assert all(isinstance(b, OHLCBar) for b in bars)

    def test_timestamps_ns_utc(self) -> None:
        bars = self.parse(self.raw)
        for b in bars:
            assert isinstance(b.ts, int)
            assert b.ts > 1_780_000_000_000_000_000

    def test_first_bar_values(self) -> None:
        """
        Fixture row 0: [1781164800000, '62719.39', '63050.00', '62719.38',
                        '62957.99', '546.929', ..., '34412021.54', 82404, ...]
        """
        bars = self.parse(self.raw)
        b = bars[0]
        assert b.ts == 1781164800000 * 1_000_000
        assert b.open == pytest.approx(62719.39)
        assert b.high == pytest.approx(63050.0)
        assert b.low == pytest.approx(62719.38)
        assert b.close == pytest.approx(62957.99)
        assert b.volume == pytest.approx(546.92925)
        assert b.quote_volume == pytest.approx(34412021.549089)
        assert b.trades == 82404

    def test_ascending_timestamps(self) -> None:
        bars = self.parse(self.raw)
        ts = [b.ts for b in bars]
        assert ts == sorted(ts)


class TestBinanceAggTradesParsing:
    """Feed binance_aggtrades_page.json through _parse_aggtrades_page."""

    def setup_method(self) -> None:
        from dccd.sources.binance import _parse_aggtrades_page
        self.parse = _parse_aggtrades_page
        self.raw = _load("binance_aggtrades_page.json")

    def test_record_count(self) -> None:
        trades, _ = self.parse(self.raw)
        assert len(trades) == 5

    def test_records_are_trades(self) -> None:
        trades, _ = self.parse(self.raw)
        assert all(isinstance(t, Trade) for t in trades)

    def test_timestamps_ns_utc(self) -> None:
        trades, _ = self.parse(self.raw)
        for t in trades:
            assert isinstance(t.ts, int)
            assert t.ts >= 1_704_067_200_000_000_000

    def test_side_mapping_maker_is_sell(self) -> None:
        """m=true means buyer is maker → taker is seller."""
        trades, _ = self.parse(self.raw)
        # fixture[0] has m=true → sell
        assert trades[0].side == "sell"
        # fixture[1] has m=false → buy
        assert trades[1].side == "buy"

    def test_tid_is_agg_id_string(self) -> None:
        trades, _ = self.parse(self.raw)
        assert trades[0].tid == "2807759891"

    def test_empty_yields_no_cursor(self) -> None:
        trades, cursor = self.parse([])
        assert trades == []
        assert cursor is None

    def test_cursor_returned_when_end_ms_not_reached(self) -> None:
        """A full page with last_ts < end_ms yields a cursor."""
        trades, cursor = self.parse(self.raw, end_ms=2_000_000_000_000)
        # last trade T=1704067200004 < end_ms → cursor = last_a + 1
        assert cursor == str(2807759895 + 1)

    def test_no_cursor_when_end_ms_reached(self) -> None:
        trades, cursor = self.parse(self.raw, end_ms=1_704_067_200_000)
        assert cursor is None


class TestBinanceWSParsing:
    """Feed binance WS fixture messages through the WS class parse_message."""

    def _collect(self, ws: object, raw: str) -> list:
        import asyncio

        async def _drain() -> list:
            records = []
            async for r in ws.parse_message(raw):
                records.append(r)
            return records

        return asyncio.get_event_loop().run_until_complete(_drain())

    def test_kline_two_messages_yield_two_bars(self) -> None:
        from dccd.sources.binance import _BinanceKlineWS
        ws = _BinanceKlineWS("btcusdt", "1h")
        msgs = _load("binance_ws_kline_msgs.json")
        records = []
        for msg in msgs[:2]:
            records.extend(self._collect(ws, json.dumps(msg)))
        assert len(records) == 2
        assert all(isinstance(r, OHLCBar) for r in records)

    def test_kline_unknown_event_ignored(self) -> None:
        from dccd.sources.binance import _BinanceKlineWS
        ws = _BinanceKlineWS("btcusdt", "1h")
        msgs = _load("binance_ws_kline_msgs.json")
        records = self._collect(ws, json.dumps(msgs[2]))  # "OTHER_EVENT"
        assert records == []

    def test_kline_values(self) -> None:
        from dccd.sources.binance import _BinanceKlineWS
        ws = _BinanceKlineWS("btcusdt", "1h")
        msgs = _load("binance_ws_kline_msgs.json")
        records = self._collect(ws, json.dumps(msgs[0]))
        b = records[0]
        assert b.ts == 1781164800000 * 1_000_000
        assert b.open == pytest.approx(62719.39)

    def test_agg_trade_sell(self) -> None:
        from dccd.sources.binance import _BinanceTradeWS
        ws = _BinanceTradeWS("btcusdt")
        msgs = _load("binance_ws_agg_trade_msgs.json")
        records = self._collect(ws, json.dumps(msgs[0]))  # m=true → sell
        assert len(records) == 1
        assert records[0].side == "sell"

    def test_agg_trade_buy(self) -> None:
        from dccd.sources.binance import _BinanceTradeWS
        ws = _BinanceTradeWS("btcusdt")
        msgs = _load("binance_ws_agg_trade_msgs.json")
        records = self._collect(ws, json.dumps(msgs[1]))  # m=false → buy
        assert len(records) == 1
        assert records[0].side == "buy"

    def test_unknown_event_ignored(self) -> None:
        from dccd.sources.binance import _BinanceTradeWS
        ws = _BinanceTradeWS("btcusdt")
        msgs = _load("binance_ws_agg_trade_msgs.json")
        records = self._collect(ws, json.dumps(msgs[2]))
        assert records == []


# ---------------------------------------------------------------------------
# Adapter symbol / stream factory coverage (non-I/O paths)
# ---------------------------------------------------------------------------

class TestBitfinexSymbolAndStream:
    """Cover render_symbol and stream method factories (no I/O)."""

    def test_render_symbol_standard(self) -> None:
        from dccd.sources.bitfinex import BitfinexSource
        src = BitfinexSource()
        assert src.render_symbol(Symbol(base="BTC", quote="USD")) == "tBTCUSD"

    def test_render_symbol_usdt_becomes_ust(self) -> None:
        from dccd.sources.bitfinex import BitfinexSource
        src = BitfinexSource()
        assert src.render_symbol(Symbol(base="BTC", quote="USDT")) == "tBTCUST"

    def test_render_symbol_long_base_uses_colon(self) -> None:
        from dccd.sources.bitfinex import BitfinexSource
        src = BitfinexSource()
        # base >3 chars → tBASE:QUOTE form
        assert src.render_symbol(Symbol(base="ALGO", quote="USD")) == "tALGO:USD"

    def test_stream_ohlc_returns_iterator(self) -> None:
        from dccd.sources.bitfinex import BitfinexSource
        src = BitfinexSource()
        it = src.stream_ohlc(Symbol(base="BTC", quote="USD"), 3600)
        # Just check it's an async iterable (coroutine/generator, not a record)
        import inspect
        assert inspect.isasyncgen(it)

    def test_stream_trades_returns_iterator(self) -> None:
        from dccd.sources.bitfinex import BitfinexSource
        src = BitfinexSource()
        it = src.stream_trades(Symbol(base="BTC", quote="USD"))
        import inspect
        assert inspect.isasyncgen(it)

    def test_stream_orderbook_raises(self) -> None:
        import pytest

        from dccd.sources.bitfinex import BitfinexSource
        src = BitfinexSource()
        with pytest.raises(NotImplementedError):
            src.stream_orderbook(Symbol(base="BTC", quote="USD"), depth=10)

    def test_ws_parse_wrong_chan_id_yields_nothing(self) -> None:
        """Messages for an unknown channel id are silently dropped."""
        import asyncio

        from dccd.sources.bitfinex import _BitfinexWS
        ws = _BitfinexWS("tBTCUSD", "trades")
        ws._chan_id = 17
        # Message for chan_id=999 (unknown)
        raw = json.dumps([999, [4161, 1358217143000, 44.0, 14.52]])

        async def _drain() -> list:
            records = []
            async for r in ws.parse_message(raw):
                records.append(r)
            return records

        records = asyncio.get_event_loop().run_until_complete(_drain())
        assert records == []

    def test_ws_parse_short_list_yields_nothing(self) -> None:
        """Lists with < 2 elements are ignored."""
        import asyncio

        from dccd.sources.bitfinex import _BitfinexWS
        ws = _BitfinexWS("tBTCUSD", "trades")
        ws._chan_id = 17
        raw = json.dumps([17])  # len < 2

        async def _drain() -> list:
            records = []
            async for r in ws.parse_message(raw):
                records.append(r)
            return records

        records = asyncio.get_event_loop().run_until_complete(_drain())
        assert records == []

    def test_ws_on_connect_sends_subscribe_for_trades(self) -> None:
        """on_connect sends a subscribe message for the trades channel."""
        import asyncio
        from unittest.mock import AsyncMock, MagicMock

        from dccd.sources.bitfinex import _BitfinexWS
        ws = _BitfinexWS("tBTCUSD", "trades")
        mock_ws = MagicMock()
        mock_ws.send = AsyncMock()
        asyncio.get_event_loop().run_until_complete(ws.on_connect(mock_ws))
        mock_ws.send.assert_called_once()
        sent = json.loads(mock_ws.send.call_args[0][0])
        assert sent["event"] == "subscribe"
        assert sent["channel"] == "trades"
        assert "key" not in sent

    def test_ws_on_connect_sends_key_for_candles(self) -> None:
        """on_connect adds 'key' for the candles channel."""
        import asyncio
        from unittest.mock import AsyncMock, MagicMock

        from dccd.sources.bitfinex import _BitfinexWS
        ws = _BitfinexWS("tBTCUSD", "candles", "1h")
        mock_ws = MagicMock()
        mock_ws.send = AsyncMock()
        asyncio.get_event_loop().run_until_complete(ws.on_connect(mock_ws))
        sent = json.loads(mock_ws.send.call_args[0][0])
        assert "key" in sent
        assert "1h" in sent["key"]


class TestBitMEXOHLCParsingEdgeCases:
    """Additional edge-case coverage for _parse_ohlc_page (bad timestamps)."""

    def test_bad_timestamp_entry_skipped(self) -> None:
        from dccd.sources.bitmex import _parse_ohlc_page
        data = [
            {"timestamp": "INVALID", "open": 1, "high": 2, "low": 0, "close": 1, "volume": 10},
            {"timestamp": "2024-01-01T00:00:00.000Z", "open": 42282.0, "high": 42374.0,
             "low": 42083.5, "close": 42312.0, "volume": 11776000},
        ]
        bars = _parse_ohlc_page(data)
        # Bad entry skipped, good one kept
        assert len(bars) == 1
        assert bars[0].open == pytest.approx(42282.0)


class TestBitMEXStreamFactories:
    """Cover BitMEX stream factories and WS helpers (no I/O)."""

    def test_stream_ohlc_returns_iterator(self) -> None:
        import inspect

        from dccd.sources.bitmex import BitMEXSource
        src = BitMEXSource()
        it = src.stream_ohlc(Symbol(base="BTC", quote="USD"), 3600)
        assert inspect.isasyncgen(it)

    def test_stream_trades_returns_iterator(self) -> None:
        import inspect

        from dccd.sources.bitmex import BitMEXSource
        src = BitMEXSource()
        it = src.stream_trades(Symbol(base="BTC", quote="USD"))
        assert inspect.isasyncgen(it)

    def test_stream_orderbook_returns_iterator(self) -> None:
        import inspect

        from dccd.sources.bitmex import BitMEXSource
        src = BitMEXSource()
        it = src.stream_orderbook(Symbol(base="BTC", quote="USD"), depth=10)
        assert inspect.isasyncgen(it)

    def test_ws_on_connect_sends_subscribe(self) -> None:
        import asyncio
        from unittest.mock import AsyncMock, MagicMock

        from dccd.sources.bitmex import _BitMEXWS
        ws = _BitMEXWS("XBTUSD", "trade", "trades")
        mock_ws = MagicMock()
        mock_ws.send = AsyncMock()
        asyncio.get_event_loop().run_until_complete(ws.on_connect(mock_ws))
        sent = json.loads(mock_ws.send.call_args[0][0])
        assert sent["op"] == "subscribe"
        assert "trade:XBTUSD" in sent["args"]

    def test_ws_parse_ohlc_message(self) -> None:
        """_BitMEXWS in ohlc mode parses a tradeBin frame."""
        import asyncio

        from dccd.sources.bitmex import _BitMEXWS
        ws = _BitMEXWS("XBTUSD", "tradeBin1h", "ohlc")
        raw = json.dumps({"table": "tradeBin1h", "action": "insert", "data": [
            {"timestamp": "2024-01-01T01:00:00.000Z", "open": 42312.0, "high": 42584.5,
             "low": 42286.5, "close": 42488.5, "volume": 10041600}
        ]})

        async def _drain() -> list:
            records = []
            async for r in ws.parse_message(raw):
                records.append(r)
            return records

        records = asyncio.get_event_loop().run_until_complete(_drain())
        assert len(records) == 1
        assert isinstance(records[0], OHLCBar)
        assert records[0].open == pytest.approx(42312.0)

    def test_ws_parse_book_message(self) -> None:
        """_BitMEXWS in book mode parses an orderBook10 frame."""
        import asyncio

        from dccd.domain.records import OrderBookSnapshot
        from dccd.sources.bitmex import _BitMEXWS
        ws = _BitMEXWS("XBTUSD", "orderBook10", "book")
        raw = json.dumps({"table": "orderBook10", "action": "update", "data": [
            {"symbol": "XBTUSD",
             "bids": [[42315.0, 100], [42310.0, 200]],
             "asks": [[42316.0, 50], [42320.0, 150]]}
        ]})

        async def _drain() -> list:
            records = []
            async for r in ws.parse_message(raw):
                records.append(r)
            return records

        records = asyncio.get_event_loop().run_until_complete(_drain())
        assert len(records) == 1
        assert isinstance(records[0], OrderBookSnapshot)
        assert len(records[0].bids) == 2
        assert len(records[0].asks) == 2

    def test_ws_parse_ohlc_bad_timestamp_skipped(self) -> None:
        """Invalid timestamps in ohlc WS frames are skipped."""
        import asyncio

        from dccd.sources.bitmex import _BitMEXWS
        ws = _BitMEXWS("XBTUSD", "tradeBin1h", "ohlc")
        raw = json.dumps({"table": "tradeBin1h", "action": "insert", "data": [
            {"timestamp": "INVALID_DATE", "open": 42312.0, "high": 42584.5,
             "low": 42286.5, "close": 42488.5, "volume": 10041600},
            {"timestamp": "2024-01-01T01:00:00.000Z", "open": 42312.0, "high": 42584.5,
             "low": 42286.5, "close": 42488.5, "volume": 10041600},
        ]})

        async def _drain() -> list:
            records = []
            async for r in ws.parse_message(raw):
                records.append(r)
            return records

        records = asyncio.get_event_loop().run_until_complete(_drain())
        assert len(records) == 1  # bad timestamp skipped

    def test_ws_parse_trades_bad_timestamp_skipped(self) -> None:
        """Invalid timestamps in trades WS frames are skipped."""
        import asyncio

        from dccd.sources.bitmex import _BitMEXWS
        ws = _BitMEXWS("XBTUSD", "trade", "trades")
        raw = json.dumps({"table": "trade", "action": "insert", "data": [
            {"timestamp": "NOT_A_DATE", "side": "Buy", "size": 100,
             "price": 42315.0, "trdMatchID": "abc"},
            {"timestamp": "2024-01-01T00:00:01.235Z", "side": "Sell", "size": 100,
             "price": 42315.0, "trdMatchID": "def"},
        ]})

        async def _drain() -> list:
            records = []
            async for r in ws.parse_message(raw):
                records.append(r)
            return records

        records = asyncio.get_event_loop().run_until_complete(_drain())
        assert len(records) == 1

    def test_ws_check_sub_ack_raises_on_success_false(self) -> None:
        """_check_sub_ack raises RuntimeError when success=False."""
        from dccd.sources.bitmex import _BitMEXWS
        ws = _BitMEXWS("XBTUSD", "trade", "trades")
        with pytest.raises(RuntimeError):
            ws._check_sub_ack({"subscribe": "trade:XBTUSD", "success": False})


class TestCoinbaseStreamFactories:
    """Cover Coinbase stream factories and WS helpers (no I/O)."""

    def test_render_symbol(self) -> None:
        from dccd.sources.coinbase import CoinbaseSource
        src = CoinbaseSource()
        assert src.render_symbol(Symbol(base="BTC", quote="USD")) == "BTC-USD"

    def test_stream_ohlc_raises(self) -> None:
        import pytest

        from dccd.sources.coinbase import CoinbaseSource
        src = CoinbaseSource()
        with pytest.raises(NotImplementedError):
            src.stream_ohlc(Symbol(base="BTC", quote="USD"), span=3600)

    def test_stream_orderbook_raises(self) -> None:
        import pytest

        from dccd.sources.coinbase import CoinbaseSource
        src = CoinbaseSource()
        with pytest.raises(NotImplementedError):
            src.stream_orderbook(Symbol(base="BTC", quote="USD"), depth=10)

    def test_stream_trades_returns_async_gen(self) -> None:
        import inspect

        from dccd.sources.coinbase import CoinbaseSource
        src = CoinbaseSource()
        it = src.stream_trades(Symbol(base="BTC", quote="USD"))
        assert inspect.isasyncgen(it)

    def test_ws_on_connect_sends_subscribe(self) -> None:
        import asyncio
        from unittest.mock import AsyncMock, MagicMock

        from dccd.sources.coinbase import _CoinbaseWS
        ws = _CoinbaseWS("BTC-USD")
        mock_ws = MagicMock()
        mock_ws.send = AsyncMock()
        asyncio.get_event_loop().run_until_complete(ws.on_connect(mock_ws))
        sent = json.loads(mock_ws.send.call_args[0][0])
        assert sent["type"] == "subscribe"
        assert "BTC-USD" in sent["product_ids"]
        assert sent["channel"] == "market_trades"

    def test_ws_parse_exception_skipped(self) -> None:
        """Malformed trades in a valid event are silently skipped."""
        import asyncio

        from dccd.sources.coinbase import _CoinbaseWS
        ws = _CoinbaseWS("BTC-USD")
        # trade missing "price" key → raises KeyError → caught
        raw = json.dumps({
            "channel": "market_trades",
            "events": [{"type": "update", "trades": [
                {"trade_id": "1", "size": "0.001", "side": "BUY",
                 "time": "2026-06-11T12:53:40.000000Z"}  # no "price"
            ]}]
        })

        async def _drain() -> list:
            records = []
            async for r in ws.parse_message(raw):
                records.append(r)
            return records

        records = asyncio.get_event_loop().run_until_complete(_drain())
        assert records == []

    def test_ws_parse_side_empty_string_becomes_none(self) -> None:
        """side='' in the WS payload maps to None."""
        import asyncio

        from dccd.sources.coinbase import _CoinbaseWS
        ws = _CoinbaseWS("BTC-USD")
        raw = json.dumps({
            "channel": "market_trades",
            "events": [{"type": "update", "trades": [
                {"trade_id": "1", "price": "62929.43", "size": "0.001",
                 "side": "", "time": "2026-06-11T12:53:40.000000Z"}
            ]}]
        })

        async def _drain() -> list:
            records = []
            async for r in ws.parse_message(raw):
                records.append(r)
            return records

        records = asyncio.get_event_loop().run_until_complete(_drain())
        assert len(records) == 1
        assert records[0].side is None


class TestBinanceSymbolAndStream:
    """Cover Binance render_symbol and stream factories (no I/O)."""

    def test_render_symbol(self) -> None:
        from dccd.sources.binance import BinanceSource
        src = BinanceSource()
        assert src.render_symbol(Symbol(base="BTC", quote="USDT")) == "BTCUSDT"

    def test_stream_ohlc_returns_iterator(self) -> None:
        import inspect

        from dccd.sources.binance import BinanceSource
        src = BinanceSource()
        it = src.stream_ohlc(Symbol(base="BTC", quote="USDT"), span=3600)
        assert inspect.isasyncgen(it)

    def test_stream_trades_returns_iterator(self) -> None:
        import inspect

        from dccd.sources.binance import BinanceSource
        src = BinanceSource()
        it = src.stream_trades(Symbol(base="BTC", quote="USDT"))
        assert inspect.isasyncgen(it)

    def test_stream_orderbook_returns_iterator(self) -> None:
        import inspect

        from dccd.sources.binance import BinanceSource
        src = BinanceSource()
        it = src.stream_orderbook(Symbol(base="BTC", quote="USDT"), depth=10)
        assert inspect.isasyncgen(it)

    def test_depth_ws_parse_depth_frame(self) -> None:
        """_BinanceDepthWS parses a partial book depth frame."""
        import asyncio

        from dccd.domain.records import OrderBookSnapshot
        from dccd.sources.binance import _BinanceDepthWS
        ws = _BinanceDepthWS("btcusdt", 20)
        raw = json.dumps({
            "bids": [["62957.99", "0.5"], ["62950.00", "1.0"]],
            "asks": [["62958.00", "0.3"], ["62960.00", "2.0"]],
        })

        async def _drain() -> list:
            records = []
            async for r in ws.parse_message(raw):
                records.append(r)
            return records

        records = asyncio.get_event_loop().run_until_complete(_drain())
        assert len(records) == 1
        assert isinstance(records[0], OrderBookSnapshot)
        assert len(records[0].bids) == 2

    def test_depth_ws_ignores_frame_without_bids(self) -> None:
        import asyncio

        from dccd.sources.binance import _BinanceDepthWS
        ws = _BinanceDepthWS("btcusdt", 20)
        raw = json.dumps({"lastUpdateId": 12345})

        async def _drain() -> list:
            records = []
            async for r in ws.parse_message(raw):
                records.append(r)
            return records

        records = asyncio.get_event_loop().run_until_complete(_drain())
        assert records == []
