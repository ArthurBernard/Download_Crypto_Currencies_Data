"""Tests for v3 storage layer."""


import pytest

from dccd.domain.dataset import DatasetId
from dccd.domain.records import OHLCBar, OrderBookLevel, OrderBookSnapshot, Trade
from dccd.domain.symbol import Symbol
from dccd.domain.timeutils import NS
from dccd.domain.types import DataType
from dccd.storage.parquet import ParquetStore
from dccd.storage.runs_sqlite import RunsStore


@pytest.fixture
def tmp_store(tmp_path):
    return ParquetStore(tmp_path)


@pytest.fixture
def ohlc_ds():
    return DatasetId(
        exchange="binance",
        symbol=Symbol(base="BTC", quote="USDT"),
        data_type=DataType.OHLC,
        span=3600,
    )


@pytest.fixture
def trades_ds():
    return DatasetId(
        exchange="binance",
        symbol=Symbol(base="BTC", quote="USDT"),
        data_type=DataType.TRADES,
        span=None,
    )


class TestParquetStore:
    def test_save_and_load_ohlc(self, tmp_store, ohlc_ds):
        bars = [
            OHLCBar(ts=i * 3600 * NS, open=1.0, high=2.0, low=0.5, close=1.5, volume=10.0)
            for i in range(10)
        ]
        n = tmp_store.save(ohlc_ds, bars)
        assert n > 0

        df = tmp_store.load(ohlc_ds)
        assert len(df) == 10
        assert "TS" in df.columns

    def test_save_and_load_trades(self, tmp_store, trades_ds):
        trades = [
            Trade(ts=i * 1000 * NS, price=50000.0 + i, amount=0.1, side="buy")
            for i in range(5)
        ]
        n = tmp_store.save(trades_ds, trades)
        assert n > 0

        df = tmp_store.load(trades_ds)
        assert len(df) == 5

    def test_save_empty(self, tmp_store, ohlc_ds):
        n = tmp_store.save(ohlc_ds, [])
        assert n == 0

    def test_dedup_on_save(self, tmp_store, ohlc_ds):
        bars = [OHLCBar(ts=3600 * NS, open=1.0, high=2.0, low=0.5, close=1.5, volume=10.0)]
        tmp_store.save(ohlc_ds, bars)
        # same TS → dedup
        tmp_store.save(ohlc_ds, bars)
        df = tmp_store.load(ohlc_ds)
        assert len(df) == 1

    def test_last_timestamp(self, tmp_store, ohlc_ds):
        assert tmp_store.last_timestamp(ohlc_ds) is None
        bars = [OHLCBar(ts=7200 * NS, open=1.0, high=2.0, low=0.5, close=1.5, volume=10.0)]
        tmp_store.save(ohlc_ds, bars)
        assert tmp_store.last_timestamp(ohlc_ds) == 7200 * NS

    def test_missing_intervals_empty(self, tmp_store, ohlc_ds):
        start = int(1_000_000 * NS)
        end = int(1_100_000 * NS)
        gaps = tmp_store.missing_intervals(ohlc_ds, start, end)
        assert len(gaps) > 0

    def test_inventory(self, tmp_store, ohlc_ds, trades_ds):
        bars = [OHLCBar(ts=3600 * NS, open=1.0, high=2.0, low=0.5, close=1.5, volume=10.0)]
        tmp_store.save(ohlc_ds, bars)
        inv = tmp_store.inventory()
        assert any(d["exchange"] == "binance" for d in inv)

    def test_inventory_bytes_and_gaps(self, tmp_store, ohlc_ds):
        # 10 hourly bars but with one hole (skip index 5) → 9 stored, 10 expected.
        bars = [
            OHLCBar(ts=i * 3600 * NS, open=1.0, high=2.0, low=0.5, close=1.5, volume=10.0)
            for i in range(10) if i != 5
        ]
        tmp_store.save(ohlc_ds, bars)
        entry = next(d for d in tmp_store.inventory() if d["data_type"] == "ohlc")
        assert entry["bytes"] > 0
        assert entry["rows"] == 9
        assert entry["expected_rows"] == 10
        assert entry["missing_rows"] == 1

    def test_inventory_trades_no_gaps(self, tmp_store, trades_ds):
        tmp_store.save(trades_ds, [Trade(ts=NS, price=1.0, amount=1.0)])
        entry = next(d for d in tmp_store.inventory() if d["data_type"] == "trades")
        assert entry["bytes"] > 0
        assert entry["missing_rows"] is None
        assert entry["span"] is None

    def test_load_with_range(self, tmp_store, ohlc_ds):
        bars = [
            OHLCBar(ts=i * 3600 * NS, open=1.0, high=2.0, low=0.5, close=1.5, volume=10.0)
            for i in range(10)
        ]
        tmp_store.save(ohlc_ds, bars)
        df = tmp_store.load(ohlc_ds, start_ns=3 * 3600 * NS, end_ns=6 * 3600 * NS)
        assert len(df) == 4  # ts 3, 4, 5, 6

    def test_save_orderbook(self, tmp_store):
        book_ds = DatasetId(
            exchange="binance",
            symbol=Symbol(base="ETH", quote="USDT"),
            data_type=DataType.ORDERBOOK,
        )
        snaps = [
            OrderBookSnapshot(
                ts=i * 60 * NS,
                bids=[OrderBookLevel(price=100.0, amount=1.0)],
                asks=[OrderBookLevel(price=100.1, amount=0.5)],
            )
            for i in range(3)
        ]
        n = tmp_store.save(book_ds, snaps)
        assert n > 0


class TestRunsStore:
    @pytest.fixture
    def runs_store(self, tmp_path):
        return RunsStore(tmp_path / "runs.db")

    def test_create_and_finish(self, runs_store):
        runs_store.create_run("r1", "spec1", "backfill", "binance", "BTC/USDT", "ohlc")
        run = runs_store.get_run("r1")
        assert run is not None
        assert run["state"] == "running"

        runs_store.finish_run("r1", "succeeded", rows_written=100)
        run = runs_store.get_run("r1")
        assert run["state"] == "succeeded"
        assert run["rows_written"] == 100

    def test_list_runs(self, runs_store):
        for i in range(5):
            runs_store.create_run(f"r{i}", "spec1", "backfill", "binance", "BTC/USDT", "ohlc")
        runs = runs_store.list_runs()
        assert len(runs) == 5

    def test_append_log(self, runs_store):
        runs_store.create_run("r1", "spec1", "backfill", "binance", "BTC/USDT", "ohlc")
        runs_store.append_log("r1", "Starting...")
        runs_store.append_log("r1", "Done.")
        run = runs_store.get_run("r1")
        import json
        log = json.loads(run["log_tail"])
        assert "Starting..." in log
        assert "Done." in log

    def test_update_progress(self, runs_store):
        runs_store.create_run("r1", "spec1", "backfill", "binance", "BTC/USDT", "ohlc")
        runs_store.update_progress("r1", {"done": 5, "total": 10})
        run = runs_store.get_run("r1")
        import json
        prog = json.loads(run["progress"])
        assert prog["done"] == 5
