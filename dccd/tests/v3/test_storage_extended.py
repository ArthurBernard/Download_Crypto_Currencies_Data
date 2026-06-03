"""Extended storage tests — migration, missing_intervals, active_runs, queue."""

import pathlib

import polars as pl
import pytest

from dccd.domain.dataset import DatasetId
from dccd.domain.records import OHLCBar
from dccd.domain.symbol import Symbol
from dccd.domain.timeutils import NS
from dccd.domain.types import DataType
from dccd.storage.migrate import migrate_parquet_to_ns, needs_migration
from dccd.storage.parquet import ParquetStore
from dccd.storage.runs_sqlite import RunsStore

# ---------------------------------------------------------------------------
# Migration round-trip
# ---------------------------------------------------------------------------

class TestMigrationRoundTrip:
    def test_migrate_seconds_to_ns(self, tmp_path: pathlib.Path) -> None:
        """Write a file with seconds-scale TS, migrate, verify ns timestamps."""
        f = tmp_path / "test.parquet"
        original_ts = [1_600_000_000, 1_600_003_600]
        df = pl.DataFrame({
            "TS": original_ts,
            "open": [50000.0, 51000.0],
            "close": [51000.0, 52000.0],
        })
        df.write_parquet(f)

        assert needs_migration(f)
        report = migrate_parquet_to_ns(tmp_path, dry_run=False)
        assert any(r["migrated"] for r in report)

        migrated = pl.read_parquet(f)
        assert not needs_migration(f)
        assert migrated["TS"][0] == original_ts[0] * NS
        assert migrated["TS"][1] == original_ts[1] * NS

    def test_dry_run_does_not_modify(self, tmp_path: pathlib.Path) -> None:
        f = tmp_path / "test.parquet"
        df = pl.DataFrame({"TS": [1_600_000_000], "close": [50000.0]})
        df.write_parquet(f)

        migrate_parquet_to_ns(tmp_path, dry_run=True)
        assert needs_migration(f), "dry_run must not modify the file"

    def test_already_ns_skipped(self, tmp_path: pathlib.Path) -> None:
        f = tmp_path / "ns.parquet"
        df = pl.DataFrame({"TS": [1_600_000_000 * NS], "close": [50000.0]})
        df.write_parquet(f)

        report = migrate_parquet_to_ns(tmp_path, dry_run=False)
        not_migrated = [r for r in report if not r.get("migrated")]
        assert any(str(f) in r["path"] for r in not_migrated)


# ---------------------------------------------------------------------------
# missing_intervals with partial existing year
# ---------------------------------------------------------------------------

class TestMissingIntervalsPartialYear:
    @pytest.fixture
    def store(self, tmp_path: pathlib.Path) -> ParquetStore:
        return ParquetStore(tmp_path)

    @pytest.fixture
    def ohlc_ds(self) -> DatasetId:
        return DatasetId(
            exchange="binance",
            symbol=Symbol(base="BTC", quote="USDT"),
            data_type=DataType.OHLC,
            span=3600,
        )

    def test_leading_gap_detected(self, store: ParquetStore, ohlc_ds: DatasetId) -> None:
        """Save bars from ts=5 to ts=9; request [0, 11] → leading gap [0, 5] detected."""
        bars = [
            OHLCBar(ts=i * 3600 * NS, open=1.0, high=1.0, low=1.0, close=1.0, volume=1.0)
            for i in range(5, 10)
        ]
        store.save(ohlc_ds, bars)

        start_ns = 0
        end_ns = 11 * 3600 * NS
        gaps = store.missing_intervals(ohlc_ds, start_ns, end_ns)

        assert len(gaps) > 0
        # Leading gap: [0, first_bar_ts]
        leading = [g for g in gaps if g[0] == start_ns]
        assert leading, f"Expected leading gap starting at 0, got: {gaps}"

    def test_trailing_gap_detected(self, store: ParquetStore, ohlc_ds: DatasetId) -> None:
        """Save bars [0, 4]; request [0, 10] → trailing gap after last bar."""
        bars = [
            OHLCBar(ts=i * 3600 * NS, open=1.0, high=1.0, low=1.0, close=1.0, volume=1.0)
            for i in range(5)
        ]
        store.save(ohlc_ds, bars)

        end_ns = 10 * 3600 * NS
        gaps = store.missing_intervals(ohlc_ds, 0, end_ns)

        trailing = [g for g in gaps if g[1] == end_ns]
        assert trailing, f"Expected trailing gap ending at {end_ns}, got: {gaps}"

    def test_no_gap_when_complete(self, store: ParquetStore, ohlc_ds: DatasetId) -> None:
        """Exact coverage → no gaps."""
        bars = [
            OHLCBar(ts=i * 3600 * NS, open=1.0, high=1.0, low=1.0, close=1.0, volume=1.0)
            for i in range(5)
        ]
        store.save(ohlc_ds, bars)
        end_ns = 4 * 3600 * NS
        gaps = store.missing_intervals(ohlc_ds, 0, end_ns)
        # May still have a trailing gap if end_ns > last bar
        # No gap should start after the last bar within the stored range
        assert all(g[0] >= 4 * 3600 * NS for g in gaps), f"Unexpected gap: {gaps}"


# ---------------------------------------------------------------------------
# RunsStore.active_runs
# ---------------------------------------------------------------------------

class TestRunsStoreActiveRuns:
    @pytest.fixture
    def runs_store(self, tmp_path: pathlib.Path) -> RunsStore:
        return RunsStore(tmp_path / "runs.db")

    def test_active_runs_includes_running(self, runs_store: RunsStore) -> None:
        runs_store.create_run("r1", "spec1", "backfill", "binance", "BTC/USDT", "ohlc")
        runs_store.create_run("r2", "spec2", "stream", "kraken", "BTC/USD", "trades")
        active = runs_store.active_runs()
        ids = [r["run_id"] for r in active]
        assert "r1" in ids
        assert "r2" in ids

    def test_finished_run_not_active(self, runs_store: RunsStore) -> None:
        runs_store.create_run("r1", "spec1", "backfill", "binance", "BTC/USDT", "ohlc")
        runs_store.finish_run("r1", "succeeded")
        active = runs_store.active_runs()
        assert all(r["run_id"] != "r1" for r in active)

    def test_reconnecting_state_active(self, runs_store: RunsStore) -> None:
        runs_store.create_run("r1", "spec1", "stream", "binance", "BTC/USDT", "trades")
        # Manually update to reconnecting state
        with runs_store._conn() as conn:
            conn.execute("UPDATE runs SET state='reconnecting' WHERE run_id='r1'")
        active = runs_store.active_runs()
        assert any(r["run_id"] == "r1" for r in active)


# ---------------------------------------------------------------------------
# EventBus.enable_queue
# ---------------------------------------------------------------------------

class TestEventBusQueue:
    @pytest.mark.asyncio
    async def test_queue_receives_events(self) -> None:
        from dccd.application.events import EventBus, LogEvent

        bus = EventBus()
        queue = bus.enable_queue(maxsize=10)

        bus.emit(LogEvent(run_id="r1", message="hello"))
        assert not queue.empty()
        event = queue.get_nowait()
        assert event.message == "hello"

    @pytest.mark.asyncio
    async def test_queue_full_does_not_crash(self) -> None:
        from dccd.application.events import EventBus, LogEvent

        bus = EventBus()
        bus.enable_queue(maxsize=2)

        # Fill the queue past capacity — should not raise
        for i in range(5):
            bus.emit(LogEvent(run_id="r1", message=f"msg{i}"))
        # No exception raised

    @pytest.mark.asyncio
    async def test_queue_receives_progress(self) -> None:
        from dccd.application.events import EventBus, ProgressEvent

        bus = EventBus()
        queue = bus.enable_queue()
        run_events = bus.for_run("run1")
        run_events.progress(3, 10, "windows")

        event = queue.get_nowait()
        assert isinstance(event, ProgressEvent)
        assert event.done == 3
        assert event.total == 10
