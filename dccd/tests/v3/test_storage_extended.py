"""Extended storage tests — missing_intervals, active_runs, queue."""

import pathlib

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dccd.domain.dataset import DatasetId
from dccd.domain.records import OHLCBar, Trade
from dccd.domain.symbol import Symbol
from dccd.domain.timeutils import NS
from dccd.domain.types import DataType
from dccd.storage.parquet import ParquetStore
from dccd.storage.runs_sqlite import RunsStore

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


# ---------------------------------------------------------------------------
# _file_stats: cache behaviour
# ---------------------------------------------------------------------------

class TestFileStatsCache:
    """Cache behaviour: mtime/size keyed, invalidates on write."""

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

    def test_cache_hit_avoids_second_parquet_open(
        self, store: ParquetStore, ohlc_ds: DatasetId, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Second inventory() call succeeds from cache even if pq.ParquetFile is broken."""
        bars = [
            OHLCBar(ts=i * 3600 * NS, open=1.0, high=1.0, low=1.0, close=1.0, volume=1.0)
            for i in range(5)
        ]
        store.save(ohlc_ds, bars)

        # First call — populates cache.
        inv1 = store.inventory()
        assert len(inv1) == 1
        entry1 = inv1[0]

        # Break pq.ParquetFile so any real file read would raise.
        def _explode(*a: object, **kw: object) -> None:
            raise RuntimeError("should not read file on cache hit")

        monkeypatch.setattr(pq, "ParquetFile", _explode)

        # Second call — must use cache, not call pq.ParquetFile.
        inv2 = store.inventory()
        assert len(inv2) == 1
        entry2 = inv2[0]
        assert entry2["rows"] == entry1["rows"]
        assert entry2["min_ts"] == entry1["min_ts"]
        assert entry2["max_ts"] == entry1["max_ts"]

    def test_cache_invalidates_after_save(
        self, store: ParquetStore, ohlc_ds: DatasetId, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """After save() (mtime changes) the cache is invalidated and new rows appear."""
        bars_first = [
            OHLCBar(ts=i * 3600 * NS, open=1.0, high=1.0, low=1.0, close=1.0, volume=1.0)
            for i in range(5)
        ]
        store.save(ohlc_ds, bars_first)

        # Populate cache with 5 rows.
        inv1 = store.inventory()
        assert inv1[0]["rows"] == 5

        # Break ParquetFile; verify cache is used.
        def _explode(*a: object, **kw: object) -> None:
            raise RuntimeError("should not read on cache hit")

        monkeypatch.setattr(pq, "ParquetFile", _explode)
        inv_cached = store.inventory()
        assert inv_cached[0]["rows"] == 5

        # Restore and add more bars (mtime will differ after atomic rename).
        monkeypatch.undo()
        bars_second = [
            OHLCBar(ts=i * 3600 * NS, open=1.0, high=1.0, low=1.0, close=1.0, volume=1.0)
            for i in range(5, 10)
        ]
        store.save(ohlc_ds, bars_second)

        inv2 = store.inventory()
        assert inv2[0]["rows"] == 10


# ---------------------------------------------------------------------------
# _file_stats: fallback for write_statistics=False
# ---------------------------------------------------------------------------

class TestFileStatsFallback:
    """Parquet files written without statistics still produce correct output."""

    @pytest.fixture
    def store(self, tmp_path: pathlib.Path) -> ParquetStore:
        return ParquetStore(tmp_path)

    @pytest.fixture
    def ohlc_ds(self) -> DatasetId:
        return DatasetId(
            exchange="testex",
            symbol=Symbol(base="ETH", quote="USDT"),
            data_type=DataType.OHLC,
            span=3600,
        )

    def test_inventory_no_statistics(
        self, store: ParquetStore, ohlc_ds: DatasetId, tmp_path: pathlib.Path
    ) -> None:
        """inventory() returns correct rows/min_ts/max_ts for no-stats files."""
        # Write a file without column statistics bypassing the normal save() path.
        year_dir = store.directory(ohlc_ds)
        file_path = year_dir / "1970.parquet"
        ts_values = [i * 3600 * 10**9 for i in range(3)]
        table = pa.table(
            {
                "TS": pa.array(ts_values, type=pa.int64()),
                "open": pa.array([1.0, 2.0, 3.0], type=pa.float64()),
                "high": pa.array([1.0, 2.0, 3.0], type=pa.float64()),
                "low": pa.array([1.0, 2.0, 3.0], type=pa.float64()),
                "close": pa.array([1.0, 2.0, 3.0], type=pa.float64()),
                "volume": pa.array([1.0, 2.0, 3.0], type=pa.float64()),
                "quote_volume": pa.array([1.0, 2.0, 3.0], type=pa.float64()),
                "trades": pa.array([1, 2, 3], type=pa.int64()),
            }
        )
        pq.write_table(table, file_path, write_statistics=False)

        inv = store.inventory()
        assert len(inv) == 1
        entry = inv[0]
        assert entry["rows"] == 3
        assert entry["min_ts"] == ts_values[0]
        assert entry["max_ts"] == ts_values[-1]

    def test_last_timestamp_no_statistics(
        self, store: ParquetStore, ohlc_ds: DatasetId
    ) -> None:
        """last_timestamp() still returns correct max_ts for no-stats files."""
        year_dir = store.directory(ohlc_ds)
        file_path = year_dir / "1970.parquet"
        ts_values = [i * 3600 * 10**9 for i in range(5)]
        table = pa.table(
            {
                "TS": pa.array(ts_values, type=pa.int64()),
                "open": pa.array([1.0] * 5, type=pa.float64()),
                "high": pa.array([1.0] * 5, type=pa.float64()),
                "low": pa.array([1.0] * 5, type=pa.float64()),
                "close": pa.array([1.0] * 5, type=pa.float64()),
                "volume": pa.array([1.0] * 5, type=pa.float64()),
                "quote_volume": pa.array([1.0] * 5, type=pa.float64()),
                "trades": pa.array([1] * 5, type=pa.int64()),
            }
        )
        pq.write_table(table, file_path, write_statistics=False)

        last_ts = store.last_timestamp(ohlc_ds)
        assert last_ts == ts_values[-1]


# ---------------------------------------------------------------------------
# Value-identity: inventory/last_timestamp/missing_intervals unchanged
# ---------------------------------------------------------------------------

class TestFileStatsValueIdentity:
    """Verify the new _file_stats path produces the same values as column reads."""

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

    @pytest.fixture
    def trades_ds(self) -> DatasetId:
        return DatasetId(
            exchange="binance",
            symbol=Symbol(base="BTC", quote="USDT"),
            data_type=DataType.TRADES,
            span=None,
        )

    def test_inventory_identity_ohlc(
        self, store: ParquetStore, ohlc_ds: DatasetId
    ) -> None:
        """inventory() rows/min_ts/max_ts/expected_rows/missing_rows match stored data."""
        # 10 hourly bars with index 5 missing → 9 rows, expected=10, missing=1
        bars = [
            OHLCBar(ts=i * 3600 * NS, open=1.0, high=2.0, low=0.5, close=1.5, volume=10.0)
            for i in range(10) if i != 5
        ]
        store.save(ohlc_ds, bars)
        entry = next(d for d in store.inventory() if d["data_type"] == "ohlc")

        assert entry["rows"] == 9
        assert entry["min_ts"] == 0
        assert entry["max_ts"] == 9 * 3600 * NS
        assert entry["expected_rows"] == 10
        assert entry["missing_rows"] == 1
        assert entry["bytes"] > 0

    def test_inventory_identity_trades(
        self, store: ParquetStore, trades_ds: DatasetId
    ) -> None:
        """Trades inventory has no gap fields, correct rows/min_ts/max_ts."""
        trades = [
            Trade(ts=i * 1000 * NS, price=50000.0, amount=0.1, side="buy")
            for i in range(5)
        ]
        store.save(trades_ds, trades)
        entry = next(d for d in store.inventory() if d["data_type"] == "trades")

        assert entry["rows"] == 5
        assert entry["min_ts"] == 0
        assert entry["max_ts"] == 4 * 1000 * NS
        assert entry["missing_rows"] is None
        assert entry["expected_rows"] is None
        assert entry["bytes"] > 0

    def test_last_timestamp_identity(
        self, store: ParquetStore, ohlc_ds: DatasetId
    ) -> None:
        """last_timestamp returns the last bar TS."""
        bars = [
            OHLCBar(ts=i * 3600 * NS, open=1.0, high=1.0, low=1.0, close=1.0, volume=1.0)
            for i in range(10)
        ]
        store.save(ohlc_ds, bars)
        assert store.last_timestamp(ohlc_ds) == 9 * 3600 * NS

    def test_missing_intervals_identity(
        self, store: ParquetStore, ohlc_ds: DatasetId
    ) -> None:
        """missing_intervals detects leading and trailing gaps correctly."""
        bars = [
            OHLCBar(ts=i * 3600 * NS, open=1.0, high=1.0, low=1.0, close=1.0, volume=1.0)
            for i in range(5, 10)
        ]
        store.save(ohlc_ds, bars)

        start_ns = 0
        end_ns = 11 * 3600 * NS
        gaps = store.missing_intervals(ohlc_ds, start_ns, end_ns)

        leading = [g for g in gaps if g[0] == start_ns]
        assert leading, f"Expected leading gap starting at 0, got: {gaps}"
        trailing = [g for g in gaps if g[1] == end_ns]
        assert trailing, f"Expected trailing gap ending at {end_ns}, got: {gaps}"
