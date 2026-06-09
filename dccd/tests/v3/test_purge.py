"""Free-space purge: drop oldest already-synced Parquet files under disk pressure."""

from __future__ import annotations

import asyncio
import os
import pathlib

from dccd.application.events import EventBus
from dccd.application.scheduler import Scheduler
from dccd.application.service_factory import build_registry, build_store
from dccd.storage.purge import free_bytes, purge_to_free_space

_GB = 1024 ** 3


class _OkRemote:
    async def sync_all(self):
        return {"r:bucket": True}


def _make_store(tmp_path: pathlib.Path) -> dict[str, pathlib.Path]:
    """Create 4 parquet files (ascending mtime) + a .dccd file to be preserved."""
    files = {}
    for i, name in enumerate(["a", "b", "c", "d"]):
        p = tmp_path / "binance" / "ohlc" / f"{name}.parquet"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"x" * ((i + 1) * 10))  # 10, 20, 30, 40 bytes
        os.utime(p, (1000 + i, 1000 + i))     # a oldest … d newest
        files[name] = p
    dcache = tmp_path / ".dccd" / "coverage.db"
    dcache.parent.mkdir(parents=True, exist_ok=True)
    dcache.write_bytes(b"manifest")
    files["dcache"] = dcache
    return files


class TestPurge:
    def test_disabled_when_min_free_zero(self, tmp_path):
        f = _make_store(tmp_path)
        res = purge_to_free_space(tmp_path, 0.0, free_fn=lambda: 0)
        assert res["removed"] == []
        assert f["a"].exists()

    def test_noop_when_already_above_floor(self, tmp_path):
        _make_store(tmp_path)
        # Plenty free → nothing removed.
        res = purge_to_free_space(tmp_path, 1.0, free_fn=lambda: 2 * _GB)
        assert res["removed"] == []

    def test_drops_oldest_first_until_floor(self, tmp_path):
        f = _make_store(tmp_path)
        target = 1 * _GB
        # Start just short by the two oldest files' sizes (10 + 20 = 30 bytes).
        start_free = target - 30
        res = purge_to_free_space(tmp_path, 1.0, free_fn=lambda: start_free)
        removed = [pathlib.Path(p).name for p in res["removed"]]
        assert removed == ["a.parquet", "b.parquet"]  # oldest first, stops at floor
        assert not f["a"].exists() and not f["b"].exists()
        assert f["c"].exists() and f["d"].exists()
        assert f["dcache"].exists()  # .dccd never touched
        assert res["freed_bytes"] == 30

    def test_dry_run_keeps_files(self, tmp_path):
        f = _make_store(tmp_path)
        res = purge_to_free_space(
            tmp_path, 1.0, dry_run=True, free_fn=lambda: 1 * _GB - 30
        )
        assert [pathlib.Path(p).name for p in res["removed"]] == ["a.parquet", "b.parquet"]
        # Nothing actually deleted.
        assert f["a"].exists() and f["b"].exists()


class TestSchedulerPurge:
    async def test_purges_after_successful_sync(self, tmp_path):
        """A successful sync cycle triggers the purge when below the floor."""
        p = tmp_path / "binance" / "ohlc" / "old.parquet"
        p.parent.mkdir(parents=True)
        p.write_bytes(b"x" * 100)
        # A floor far above real free space → purge must drop the file.
        huge = free_bytes(tmp_path) / _GB + 1000
        sched = Scheduler(
            build_registry(), build_store(tmp_path), None, EventBus(),
            remote=_OkRemote(), sync_interval=0.02,
            data_path=str(tmp_path), min_free_gb=huge,
        )
        await sched.start([])
        await asyncio.sleep(0.1)
        await sched.stop()
        assert not p.exists()  # purged after sync

    async def test_no_purge_without_floor(self, tmp_path):
        p = tmp_path / "binance" / "ohlc" / "keep.parquet"
        p.parent.mkdir(parents=True)
        p.write_bytes(b"x" * 100)
        sched = Scheduler(
            build_registry(), build_store(tmp_path), None, EventBus(),
            remote=_OkRemote(), sync_interval=0.02,
            data_path=str(tmp_path), min_free_gb=0.0,
        )
        await sched.start([])
        await asyncio.sleep(0.1)
        await sched.stop()
        assert p.exists()  # no floor → never purged
