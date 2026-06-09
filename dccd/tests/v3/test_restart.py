"""Restart safety regression guards.

Durable state must survive a process restart and the scheduler must re-arm its
recurring work from config alone (so a reboot resumes with no manual step and no
gap). Verified live on a real `systemctl reboot` of a server in PR #XX — these are
the cheap regression guards for that behaviour.
"""

import pytest

from dccd.application.jobs import JobSpec, JobTarget, Trigger
from dccd.application.scheduler import Scheduler
from dccd.domain.symbol import Symbol
from dccd.domain.types import DataType
from dccd.storage.runs_sqlite import RunsStore


def _interval_spec(every: int = 120) -> JobSpec:
    target = JobTarget(
        exchange="binance",
        symbol=Symbol(base="BTC", quote="USDT"),
        data_type=DataType.OHLC,
        span=3600,
    )
    return JobSpec(
        id=JobSpec.make_id("backfill", target),
        operation="backfill",
        target=target,
        trigger=Trigger(kind="interval", every=every),
    )


def test_runsstore_survives_reopen(tmp_path):
    """A fresh RunsStore at the same path keeps prior runs (append, not truncate)."""
    db = tmp_path / "runs.db"
    s1 = RunsStore(db)
    s1.create_run("r1", "spec-1", "backfill", "binance", "BTC/USDT", "ohlc")
    s1.finish_run("r1", "succeeded", rows_written=10)
    s1.create_run("r2", "spec-1", "backfill", "binance", "BTC/USDT", "ohlc")
    s1.finish_run("r2", "succeeded", rows_written=5)

    # New instance at the same path == a daemon restart.
    s2 = RunsStore(db)
    assert {r["run_id"] for r in s2.list_runs()} == {"r1", "r2"}

    # A post-restart run appends; the history is not reset.
    s2.create_run("r3", "spec-1", "backfill", "binance", "BTC/USDT", "ohlc")
    assert len(RunsStore(db).list_runs()) == 3


@pytest.mark.asyncio
async def test_scheduler_rearms_intervals_from_specs():
    """A fresh Scheduler re-arms the same interval loop from the same specs.

    This is the boot reconstruction path: `cmd_start` rebuilds everything from
    config and calls `scheduler.start(cfg.all_job_specs())`. No cross-process
    in-memory state — the spec id is the only thing that carries over.
    """
    spec = _interval_spec()

    async def _noop(_spec):
        return None

    sched = Scheduler(registry=None, store=None)  # type: ignore[arg-type]
    sched._run_once = _noop  # type: ignore[assignment]
    sched._running = True
    await sched.sync_intervals([spec])
    assert spec.id in sched._interval_loops
    await sched.stop()

    # A second, independent Scheduler from the same spec arms it identically.
    sched2 = Scheduler(registry=None, store=None)  # type: ignore[arg-type]
    sched2._run_once = _noop  # type: ignore[assignment]
    sched2._running = True
    await sched2.sync_intervals([spec])
    assert spec.id in sched2._interval_loops
    await sched2.stop()
