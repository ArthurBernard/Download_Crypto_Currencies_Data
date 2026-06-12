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

    def test_mark_stale_running(self, runs_store):
        """mark_stale_running transitions all running rows; done rows are untouched."""
        runs_store.create_run("r1", "spec1", "backfill", "binance", "BTC/USDT", "ohlc")
        runs_store.create_run("r2", "spec1", "backfill", "binance", "ETH/USDT", "ohlc")
        runs_store.create_run("r3", "spec1", "backfill", "binance", "XRP/USDT", "ohlc")
        runs_store.finish_run("r3", "done", rows_written=42)

        count = runs_store.mark_stale_running()

        assert count == 2, "exactly the two running rows should be updated"
        assert runs_store.active_runs() == [], "active_runs() must be empty after purge"

        r1 = runs_store.get_run("r1")
        r2 = runs_store.get_run("r2")
        assert r1 is not None and r1["state"] == "stale"
        assert r1["ended_at"] is not None, "ended_at must be set"
        assert r1["error"] == "orphaned by daemon restart"
        assert r2 is not None and r2["state"] == "stale"

        r3 = runs_store.get_run("r3")
        assert r3 is not None and r3["state"] == "done", "finished run must be untouched"

    def test_mark_stale_running_idempotent(self, runs_store):
        """Calling mark_stale_running on a clean store returns 0."""
        count = runs_store.mark_stale_running()
        assert count == 0

    def test_prune_old_runs(self, runs_store):
        """prune_old_runs(90) deletes old terminal non-failed rows; keeps old failed + recent."""
        import time
        now_ns = int(time.time() * 1_000_000_000)
        old_ns = now_ns - int(100 * 86400 * 1_000_000_000)  # 100 days ago
        recent_ns = now_ns - int(1 * 86400 * 1_000_000_000)   # 1 day ago

        # Old terminal non-failed rows — should be pruned
        for run_id, state in [
            ("old-succeeded", "succeeded"),
            ("old-stale", "stale"),
            ("old-cancelled", "cancelled"),
        ]:
            runs_store.create_run(run_id, "spec1", "backfill", "binance", "BTC/USDT", "ohlc", started_at=old_ns)
            runs_store.finish_run(run_id, state)

        # Old failed row — must NOT be pruned (kept as error journal)
        runs_store.create_run("old-failed", "spec1", "backfill", "binance", "BTC/USDT", "ohlc", started_at=old_ns)
        runs_store.finish_run("old-failed", "failed", error="something went wrong")

        # Recent succeeded row — must NOT be pruned (within retention window)
        runs_store.create_run("recent-succeeded", "spec1", "backfill", "binance", "BTC/USDT", "ohlc", started_at=recent_ns)
        runs_store.finish_run("recent-succeeded", "succeeded")

        deleted = runs_store.prune_old_runs(90)

        assert deleted == 3, f"expected 3 pruned rows, got {deleted}"
        assert runs_store.get_run("old-succeeded") is None, "old succeeded must be pruned"
        assert runs_store.get_run("old-stale") is None, "old stale must be pruned"
        assert runs_store.get_run("old-cancelled") is None, "old cancelled must be pruned"
        assert runs_store.get_run("old-failed") is not None, "old failed must be kept"
        assert runs_store.get_run("recent-succeeded") is not None, "recent succeeded must be kept"

    def test_prune_old_runs_disabled(self, runs_store):
        """prune_old_runs(0) deletes nothing."""
        import time
        now_ns = int(time.time() * 1_000_000_000)
        old_ns = now_ns - int(200 * 86400 * 1_000_000_000)  # 200 days ago

        runs_store.create_run("old-r1", "spec1", "backfill", "binance", "BTC/USDT", "ohlc", started_at=old_ns)
        runs_store.finish_run("old-r1", "succeeded")

        deleted = runs_store.prune_old_runs(0)

        assert deleted == 0, "prune_old_runs(0) must return 0"
        assert runs_store.get_run("old-r1") is not None, "row must survive when pruning is disabled"
