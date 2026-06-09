"""Tests for daemon-scheduled rclone remote sync.

Covers the wiring added so ``dccd start`` actually drives the existing
:class:`~dccd.storage.remote.RemoteStorage` on a periodic loop (it was
implemented but never instantiated outside tests). No rclone is invoked — a fake
remote stands in for ``sync_all``.
"""

from __future__ import annotations

import asyncio

from dccd.application.config import AppConfig, RemoteConfig, StorageConfig
from dccd.application.events import EventBus, LogEvent, StatusEvent
from dccd.application.scheduler import Scheduler
from dccd.application.service_factory import (
    build_registry,
    build_remote,
    build_runs_store,
    build_store,
)
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
