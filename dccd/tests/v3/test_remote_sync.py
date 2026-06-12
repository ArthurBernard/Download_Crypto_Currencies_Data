"""Tests for daemon-scheduled rclone remote sync.

Covers the wiring added so ``dccd start`` actually drives the existing
:class:`~dccd.storage.remote.RemoteStorage` on a periodic loop (it was
implemented but never instantiated outside tests). No rclone is invoked — a fake
remote stands in for ``sync_all``.

Also covers the rclone verb contract: ``sync_one`` must use ``rclone copy``
(not ``rclone sync``) so that files purged locally to reclaim disk are never
deleted from the remote archive.
"""

from __future__ import annotations

import asyncio
import pathlib
import shutil
import subprocess
import time

from dccd.application.config import AppConfig, RemoteConfig, StorageConfig
from dccd.application.events import EventBus, LogEvent, StatusEvent
from dccd.application.operations import sync_remote
from dccd.application.scheduler import Scheduler
from dccd.application.service_factory import (
    build_registry,
    build_remote,
    build_runs_store,
    build_store,
)
from dccd.storage.purge import purge_to_free_space
from dccd.storage.remote import RemoteStorage


class _FakeRemote:
    """Stand-in for RemoteStorage.sync_all that counts calls (no rclone)."""

    def __init__(self, result: dict[str, bool] | None = None, raises: bool = False):
        self._result = result if result is not None else {"r:bucket": True}
        self._raises = raises
        self.calls = 0

    async def sync_all(self) -> dict[str, bool]:
        self.calls += 1
        if self._raises:
            raise RuntimeError("boom")
        return self._result


def _scheduler(tmp_path, *, remote, sync_interval=0.02, runs_store=None, bus=None):
    return Scheduler(
        build_registry(),
        build_store(tmp_path),
        runs_store,
        bus or EventBus(),
        remote=remote,
        sync_interval=sync_interval,
    )


# ---------------------------------------------------------------------------
# build_remote factory
# ---------------------------------------------------------------------------

class TestBuildRemote:
    def test_none_without_remotes(self):
        assert build_remote(AppConfig()) is None

    def test_remote_rooted_at_data_path(self):
        cfg = AppConfig(
            settings={"data_path": "/tmp/dccd-store"},
            storage=StorageConfig(remotes=[RemoteConfig(remote="myremote:bucket")]),
        )
        remote = build_remote(cfg)
        assert isinstance(remote, RemoteStorage)
        # Local root is settings.data_path (the canonical store root).
        assert str(remote._local) == "/tmp/dccd-store"


# ---------------------------------------------------------------------------
# Scheduler sync loop
# ---------------------------------------------------------------------------

class TestSyncLoop:
    async def test_daemon_drives_sync(self, tmp_path):
        """The regression guard: start() must schedule the sync loop."""
        fake = _FakeRemote()
        sched = _scheduler(tmp_path, remote=fake)
        await sched.start([])
        await asyncio.sleep(0.1)
        await sched.stop()
        assert fake.calls >= 1
        assert sched._sync_task is None

    async def test_no_remote_no_task(self, tmp_path):
        sched = _scheduler(tmp_path, remote=None)
        await sched.start([])
        assert sched._sync_task is None
        await sched.stop()  # clean

    async def test_failure_surfaces_and_loop_survives(self, tmp_path):
        bus = EventBus()
        queue = bus.add_queue()
        runs_store = build_runs_store(tmp_path)
        fake = _FakeRemote(result={"r:bucket": False})
        sched = _scheduler(tmp_path, remote=fake, runs_store=runs_store, bus=bus)
        await sched.start([])
        await asyncio.sleep(0.1)
        await sched.stop()

        # Loop kept running despite failures.
        assert fake.calls >= 1
        # A failed status + an error log were emitted on the remote-sync channel.
        events = []
        while not queue.empty():
            events.append(queue.get_nowait())
        assert any(
            isinstance(e, StatusEvent) and e.run_id == "remote-sync"
            and e.state == "failed"
            for e in events
        )
        assert any(
            isinstance(e, LogEvent) and e.level == "error" for e in events
        )
        # And the failure was persisted as a `sync` run.
        runs = runs_store.list_runs(limit=50)
        assert any(r["operation"] == "sync" and r["state"] == "failed" for r in runs)

    async def test_success_persisted(self, tmp_path):
        runs_store = build_runs_store(tmp_path)
        fake = _FakeRemote(result={"r:bucket": True})
        sched = _scheduler(tmp_path, remote=fake, runs_store=runs_store)
        await sched.start([])
        await asyncio.sleep(0.1)
        await sched.stop()
        runs = runs_store.list_runs(limit=50)
        assert any(
            r["operation"] == "sync" and r["state"] == "succeeded" for r in runs
        )


# ---------------------------------------------------------------------------
# sync_remote — the shared one-cycle primitive (loop + manual endpoint reuse it)
# ---------------------------------------------------------------------------

class TestSyncRemoteOnce:
    async def test_success(self, tmp_path):
        runs_store = build_runs_store(tmp_path)
        bus = EventBus()
        queue = bus.add_queue()
        result = await sync_remote(
            _FakeRemote(result={"r:bucket": True}),
            runs_store=runs_store,
            events=bus.for_run("remote-sync"),
        )
        assert result["ok"] is True
        runs = runs_store.list_runs(spec_id="remote-sync", limit=1)
        assert runs and runs[0]["state"] == "succeeded"
        events = []
        while not queue.empty():
            events.append(queue.get_nowait())
        assert any(
            isinstance(e, StatusEvent) and e.state == "succeeded" for e in events
        )

    async def test_failure_persists_and_returns_not_ok(self, tmp_path):
        runs_store = build_runs_store(tmp_path)
        result = await sync_remote(
            _FakeRemote(result={"r:bucket": False}),
            runs_store=runs_store,
        )
        assert result["ok"] is False
        runs = runs_store.list_runs(spec_id="remote-sync", limit=1)
        assert runs and runs[0]["state"] == "failed"

    async def test_exception_is_caught(self, tmp_path):
        runs_store = build_runs_store(tmp_path)
        result = await sync_remote(_FakeRemote(raises=True), runs_store=runs_store)
        assert result["ok"] is False
        runs = runs_store.list_runs(spec_id="remote-sync", limit=1)
        assert runs and runs[0]["state"] == "failed"


# ---------------------------------------------------------------------------
# Rclone verb contract
# ---------------------------------------------------------------------------


def _make_parquet(path: pathlib.Path, content: bytes = b"PAR1") -> None:
    """Write a minimal stub parquet file at *path*."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def _fake_subprocess_run_copy(src_dir: pathlib.Path, dst_dir: pathlib.Path):
    """Return a fake subprocess.run that applies ``rclone copy`` semantics.

    Copy semantics: copy new/changed files from *src_dir* to *dst_dir* but
    never delete files that exist only in *dst_dir*.
    """
    def _run(cmd, **kwargs):
        # cmd is expected to be: ["rclone", verb, src, dst, ...]
        assert cmd[0] == "rclone"
        verb = cmd[1]
        src = pathlib.Path(cmd[2])
        dst = pathlib.Path(cmd[3])
        dst.mkdir(parents=True, exist_ok=True)
        if verb == "copy":
            # Copy src → dst; never delete from dst.
            for f in src.rglob("*"):
                if f.is_file():
                    rel = f.relative_to(src)
                    target = dst / rel
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(str(f), str(target))
        elif verb == "sync":
            # Mirror: copy new/changed AND delete files not in src.
            for f in src.rglob("*"):
                if f.is_file():
                    rel = f.relative_to(src)
                    target = dst / rel
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(str(f), str(target))
            for f in list(dst.rglob("*")):
                if f.is_file():
                    rel = f.relative_to(dst)
                    if not (src / rel).exists():
                        f.unlink()
        else:
            raise ValueError(f"Unexpected rclone verb: {verb!r}")
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

    return _run


class TestSyncOneVerb:
    """Guard that sync_one always calls rclone copy, never rclone sync."""

    def test_sync_one_uses_copy_verb(self, tmp_path, monkeypatch):
        """subprocess.run must be called with 'rclone copy', not 'rclone sync'."""
        captured: list[list[str]] = []

        def _fake_run(cmd, **kwargs):
            captured.append(list(cmd))
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

        monkeypatch.setattr(subprocess, "run", _fake_run)
        store = RemoteStorage(tmp_path, remotes=[{"remote": "r:bucket"}])
        store.sync_one("r:bucket")

        assert captured, "subprocess.run was never called"
        assert captured[0][0] == "rclone"
        assert captured[0][1] == "copy", (
            f"Expected 'rclone copy' but got 'rclone {captured[0][1]}'"
        )


class TestPurgeThenSyncPreservesRemoteCopy:
    """Round-trip guard: purge + copy must never destroy the only remote copy."""

    def test_purge_then_sync_preserves_remote_copy(self, tmp_path, monkeypatch):
        # ── Set up local store with two parquet files ──────────────────────
        local = tmp_path / "local"
        remote_dir = tmp_path / "remote"

        file_old = local / "binance" / "ohlc" / "btc_usdt" / "1m" / "2023.parquet"
        file_new = local / "binance" / "ohlc" / "btc_usdt" / "1m" / "2024.parquet"

        old_content = b"PAR1-old-file-content"
        new_content = b"PAR1-new-file-content"

        # Oldest file has an older mtime; sleep ensures distinct mtimes.
        _make_parquet(file_old, old_content)
        time.sleep(0.02)
        _make_parquet(file_new, new_content)

        # ── Seed the "remote" with both files (simulate a completed sync) ──
        rel_old = file_old.relative_to(local)
        rel_new = file_new.relative_to(local)
        remote_old = remote_dir / rel_old
        remote_new = remote_dir / rel_new
        _make_parquet(remote_old, old_content)
        _make_parquet(remote_new, new_content)

        # ── Purge: force deletion of the oldest local file ─────────────────
        # free_fn returns 0 bytes free (below target). The loop deletes files
        # then checks "start_free + freed >= target" BEFORE the next file.
        # If target == len(old_content), after freeing the old file we get:
        #   0 + 21 >= 21 → True → break (new file survives).
        target_bytes = len(old_content)  # = 21 bytes
        target_gb = target_bytes / (1024 ** 3)

        result = purge_to_free_space(local, min_free_gb=target_gb, free_fn=lambda: 0)

        assert len(result["removed"]) == 1, "Expected exactly one file to be purged"
        assert not file_old.exists(), "Oldest local file should have been purged"
        assert file_new.exists(), "Newest local file must still be present locally"

        # ── Run sync_one with fake-verb-semantics subprocess ───────────────
        store = RemoteStorage(local, remotes=[{"remote": str(remote_dir)}])
        monkeypatch.setattr(
            subprocess, "run",
            _fake_subprocess_run_copy(local, remote_dir),
        )
        ok = store.sync_one(str(remote_dir))
        assert ok, "sync_one should return True"

        # ── Assert: purged file STILL exists on remote ─────────────────────
        assert remote_old.exists(), (
            "Purged file must survive on the remote after rclone copy — "
            "rclone sync would have deleted it."
        )
        assert remote_old.read_bytes() == old_content

        # ── Assert: restore() brings the purged file back locally ──────────
        # Restore uses "rclone copy {remote}/{rel_path} {local}/{rel_path}".
        # We monkeypatch subprocess.run to apply copy semantics in the reverse
        # direction (remote subdir → local subdir).
        rel_dataset = str(rel_old.parent)  # directory containing the parquet

        def _fake_restore_run(cmd, **kwargs):
            assert cmd[0] == "rclone"
            assert cmd[1] == "copy"
            src_path = pathlib.Path(cmd[2])
            dst_path = pathlib.Path(cmd[3])
            dst_path.mkdir(parents=True, exist_ok=True)
            for f in src_path.rglob("*"):
                if f.is_file():
                    rel = f.relative_to(src_path)
                    target = dst_path / rel
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(str(f), str(target))
            return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")

        monkeypatch.setattr(subprocess, "run", _fake_restore_run)
        restored = store.restore(rel_dataset)
        assert restored, "restore() should return True"
        assert file_old.exists(), "Restored file must be back in the local store"
        assert file_old.read_bytes() == old_content, "Restored content must match original"
