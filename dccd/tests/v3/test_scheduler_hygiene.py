"""Tests for scheduler backoff + startup jitter and HealthMonitor alert cooldown."""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest

from dccd.application.jobs import JobSpec, JobTarget, Trigger
from dccd.domain.symbol import Symbol
from dccd.domain.types import DataType

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_spec(every: int = 30) -> JobSpec:
    target = JobTarget(
        exchange="binance",
        symbol=Symbol(base="BTC", quote="USDT"),
        data_type=DataType.OHLC,
        span=60,
    )
    return JobSpec(
        id=JobSpec.make_id("backfill", target),
        operation="backfill",
        target=target,
        trigger=Trigger(kind="interval", every=every),
    )


# ---------------------------------------------------------------------------
# Scheduler: startup jitter + failure backoff
# ---------------------------------------------------------------------------

class TestSchedulerBackoffAndJitter:
    """_interval_loop records the right sleep delays."""

    @pytest.mark.asyncio
    async def test_startup_jitter_is_within_range(self):
        """First sleep before any run is within [0, min(every, 60)]."""
        from dccd.application.scheduler import Scheduler

        every = 30
        spec = _make_spec(every=every)

        sleep_calls: list[float] = []
        run_calls: list[Any] = []
        run_count = 0

        async def fake_sleep(delay: float) -> None:
            sleep_calls.append(delay)

        async def fake_run_once(s: JobSpec) -> bool:
            nonlocal run_count
            run_count += 1
            run_calls.append(s)
            # Stop the scheduler loop after first run so the test terminates.
            sched._running = False
            return True  # success

        sched = Scheduler(registry=None, store=None)  # type: ignore[arg-type]
        sched._run_once = fake_run_once  # type: ignore[assignment]
        sched._running = True

        with patch("dccd.application.scheduler.asyncio.sleep", side_effect=fake_sleep):
            await sched._interval_loop(spec)

        # First sleep is jitter (before any run).
        assert len(sleep_calls) >= 1
        jitter = sleep_calls[0]
        assert 0.0 <= jitter <= min(every, 60)
        # A run happened.
        assert run_count >= 1

    @pytest.mark.asyncio
    async def test_failure_backoff_grows_exponentially(self):
        """On consecutive failures sleep grows every, 2*every, 4*every … capped."""
        from dccd.application.scheduler import Scheduler

        every = 30
        # Cap = max(every, 6*3600) = 21600.
        cap = max(every, 6 * 3600)
        spec = _make_spec(every=every)

        sleep_calls: list[float] = []
        fail_count = 0

        async def fake_sleep(delay: float) -> None:
            sleep_calls.append(delay)

        async def fake_run_once(s: JobSpec) -> bool:
            nonlocal fail_count
            fail_count += 1
            # Fail for the first 6 calls, then stop.
            if fail_count >= 6:
                sched._running = False
            return False  # always fail

        sched = Scheduler(registry=None, store=None)  # type: ignore[arg-type]
        sched._run_once = fake_run_once  # type: ignore[assignment]
        sched._running = True

        with patch("dccd.application.scheduler.asyncio.sleep", side_effect=fake_sleep):
            await sched._interval_loop(spec)

        # sleep_calls[0] is the startup jitter.
        # sleep_calls[1] is the first post-run delay (k=0 → every).
        # sleep_calls[2] is k=1 → 2*every, etc.
        post_run_delays = sleep_calls[1:]  # strip jitter

        # Check the delay pattern: every * 2**k, capped.
        for i, delay in enumerate(post_run_delays):
            expected = min(every * (2 ** i), cap)
            assert delay == pytest.approx(expected), (
                f"sleep_calls[{i+1}]={delay} expected {expected}"
            )

    @pytest.mark.asyncio
    async def test_success_resets_backoff(self):
        """A success after failures resets the next sleep to `every`."""
        from dccd.application.scheduler import Scheduler

        every = 30
        spec = _make_spec(every=every)

        sleep_calls: list[float] = []
        call_count = 0

        async def fake_sleep(delay: float) -> None:
            sleep_calls.append(delay)

        async def fake_run_once(s: JobSpec) -> bool:
            nonlocal call_count
            call_count += 1
            if call_count >= 4:
                sched._running = False
            # fail twice, succeed on 3rd, then stop.
            return call_count == 3

        sched = Scheduler(registry=None, store=None)  # type: ignore[arg-type]
        sched._run_once = fake_run_once  # type: ignore[assignment]
        sched._running = True

        with patch("dccd.application.scheduler.asyncio.sleep", side_effect=fake_sleep):
            await sched._interval_loop(spec)

        # sleep_calls[0] = jitter
        # sleep_calls[1] = k=0 → every (first failure)
        # sleep_calls[2] = k=1 → 2*every (second failure)
        # sleep_calls[3] = every (success resets)
        post_run = sleep_calls[1:]
        assert post_run[0] == pytest.approx(every)       # first failure: k=0
        assert post_run[1] == pytest.approx(2 * every)   # second failure: k=1
        assert post_run[2] == pytest.approx(every)       # success: reset


# ---------------------------------------------------------------------------
# HealthMonitor: alert cooldown
# ---------------------------------------------------------------------------

class TestHealthMonitorCooldown:
    """HealthMonitor fires at threshold then at most once per cooldown window."""

    def test_10_failures_fire_exactly_1_alert_within_cooldown(self, monkeypatch):
        """With max_consecutive_errors=3, 10 consecutive failures trigger exactly 1 alert."""
        import urllib.request

        from dccd.application.events import EventBus, StatusEvent
        from dccd.application.monitor import HealthMonitor

        # Freeze monotonic: cooldown never elapses.
        monkeypatch.setattr("dccd.application.monitor.time.monotonic", lambda: 0.0)

        class _Resp:
            def __enter__(self): return self
            def __exit__(self, *a): return False

        monkeypatch.setattr(urllib.request, "urlopen", lambda req, timeout=0: _Resp())
        # Count log-level error calls (the alert log).
        import logging
        logged: list[str] = []
        orig_error = logging.Logger.error

        def capture_error(self, msg, *args, **kwargs):
            logged.append(msg % args if args else str(msg))
            return orig_error(self, msg, *args, **kwargs)

        monkeypatch.setattr(logging.Logger, "error", capture_error)

        bus = EventBus()
        HealthMonitor(None, bus, webhook_url="http://hook.test", max_consecutive_errors=3)

        job = "backfill:binance:FOO/USDT:ohlc:30s"
        for i in range(10):
            bus.emit(StatusEvent(run_id=f"{job}@{i}", state="failed"))

        # Only 1 alert should have fired (at count==3; rest suppressed by cooldown).
        alert_logs = [msg for msg in logged if "FOO/USDT" in msg]
        assert len(alert_logs) == 1, f"Expected 1 alert, got {len(alert_logs)}: {alert_logs}"

    def test_cooldown_elapsed_triggers_another_alert(self, monkeypatch):
        """After cooldown elapses, 1 more alert fires while job keeps failing."""
        import urllib.request

        from dccd.application.events import EventBus, StatusEvent
        from dccd.application.monitor import HealthMonitor

        # Controllable monotonic clock.
        now = [0.0]
        monkeypatch.setattr("dccd.application.monitor.time.monotonic", lambda: now[0])

        import logging
        logged: list[str] = []
        orig_error = logging.Logger.error

        def capture_error(self, msg, *args, **kwargs):
            logged.append(msg % args if args else str(msg))
            return orig_error(self, msg, *args, **kwargs)

        monkeypatch.setattr(logging.Logger, "error", capture_error)

        class _Resp:
            def __enter__(self): return self
            def __exit__(self, *a): return False

        monkeypatch.setattr(urllib.request, "urlopen", lambda req, timeout=0: _Resp())

        bus = EventBus()
        HealthMonitor(None, bus, webhook_url="http://hook.test", max_consecutive_errors=3)

        job = "backfill:binance:FOO/USDT:ohlc:30s"
        # Trip the threshold.
        for i in range(3):
            bus.emit(StatusEvent(run_id=f"{job}@{i}", state="failed"))

        alert_logs = [msg for msg in logged if "FOO/USDT" in msg]
        assert len(alert_logs) == 1

        # More failures within cooldown → still 1.
        for i in range(3, 6):
            bus.emit(StatusEvent(run_id=f"{job}@{i}", state="failed"))
        alert_logs = [msg for msg in logged if "FOO/USDT" in msg]
        assert len(alert_logs) == 1

        # Advance clock past cooldown.
        from dccd.application.monitor import _ALERT_COOLDOWN_S
        now[0] = _ALERT_COOLDOWN_S + 1.0

        # One more failure → second alert.
        bus.emit(StatusEvent(run_id=f"{job}@6", state="failed"))
        alert_logs = [msg for msg in logged if "FOO/USDT" in msg]
        assert len(alert_logs) == 2

        # Immediately another failure → still 2 (cooldown restarted).
        now[0] = _ALERT_COOLDOWN_S + 1.5
        bus.emit(StatusEvent(run_id=f"{job}@7", state="failed"))
        alert_logs = [msg for msg in logged if "FOO/USDT" in msg]
        assert len(alert_logs) == 2

    def test_success_resets_count_and_cooldown(self, monkeypatch):
        """After a success the count resets; the next 3 failures alert again."""
        import urllib.request

        from dccd.application.events import EventBus, StatusEvent
        from dccd.application.monitor import HealthMonitor

        # Frozen monotonic — cooldown never elapses.
        monkeypatch.setattr("dccd.application.monitor.time.monotonic", lambda: 0.0)

        import logging
        logged: list[str] = []
        orig_error = logging.Logger.error

        def capture_error(self, msg, *args, **kwargs):
            logged.append(msg % args if args else str(msg))
            return orig_error(self, msg, *args, **kwargs)

        monkeypatch.setattr(logging.Logger, "error", capture_error)

        class _Resp:
            def __enter__(self): return self
            def __exit__(self, *a): return False

        monkeypatch.setattr(urllib.request, "urlopen", lambda req, timeout=0: _Resp())

        bus = EventBus()
        HealthMonitor(None, bus, webhook_url="http://hook.test", max_consecutive_errors=3)

        job = "backfill:binance:FOO/USDT:ohlc:30s"
        # Trip the threshold → 1 alert.
        for i in range(3):
            bus.emit(StatusEvent(run_id=f"{job}@{i}", state="failed"))
        alert_logs = [msg for msg in logged if "FOO/USDT" in msg]
        assert len(alert_logs) == 1

        # Success resets.
        bus.emit(StatusEvent(run_id=f"{job}@3", state="succeeded"))

        # Next 3 failures should trigger another alert.
        for i in range(4, 7):
            bus.emit(StatusEvent(run_id=f"{job}@{i}", state="failed"))
        alert_logs = [msg for msg in logged if "FOO/USDT" in msg]
        assert len(alert_logs) == 2
