"""Async scheduler + stream supervisor."""

from __future__ import annotations

import asyncio
import logging
import time

from dccd.application.events import EventBus
from dccd.application.jobs import JobSpec
from dccd.sources.registry import SourceRegistry
from dccd.storage.parquet import ParquetStore
from dccd.storage.runs_sqlite import RunsStore

__all__ = ["Scheduler"]

logger = logging.getLogger(__name__)


class _StreamWorker:
    """Supervised stream job wrapper."""

    def __init__(
        self,
        spec: JobSpec,
        registry: SourceRegistry,
        store: ParquetStore,
        runs_store: RunsStore | None,
        events: EventBus,
    ) -> None:
        self._spec = spec
        self._registry = registry
        self._store = store
        self._runs_store = runs_store
        self._events = events
        self._task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()

    @property
    def is_running(self) -> bool:
        """Whether the supervised stream task is currently alive."""
        return self._task is not None and not self._task.done()

    def start(self) -> None:
        """Start all enabled specs (streams supervised, intervals looped, once run)."""
        if self.is_running:
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_forever())

    async def stop(self) -> None:
        """Cancel interval tasks and stop every stream worker."""
        self._stop_event.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None

    async def _run_forever(self) -> None:
        from dccd.application.operations import stream
        delay = 5.0
        while not self._stop_event.is_set():
            try:
                run_events = self._events.for_run(f"{self._spec.id}@stream")
                await stream(
                    self._spec,
                    registry=self._registry,
                    store=self._store,
                    runs_store=self._runs_store,
                    events=run_events,
                    stop_event=self._stop_event,
                )
            except asyncio.CancelledError:
                return
            except Exception as exc:
                logger.warning("Stream %s failed: %s — restarting in %ds", self._spec.id, exc, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)


class Scheduler:
    """Orchestrates JobSpecs (intervals → backfill; supervised → streams).

    Parameters
    ----------
    registry : SourceRegistry
    store : ParquetStore
    runs_store : RunsStore or None
    events : EventBus
    """

    def __init__(
        self,
        registry: SourceRegistry,
        store: ParquetStore,
        runs_store: RunsStore | None = None,
        events: EventBus | None = None,
    ) -> None:
        self._registry = registry
        self._store = store
        self._runs_store = runs_store
        self._events = events or EventBus()
        self._streams: dict[str, _StreamWorker] = {}
        self._interval_tasks: list[asyncio.Task[None]] = []
        # Per-spec recurring backfill loops, keyed by spec id, with the interval
        # they were started for — so sync_intervals can reconcile (start new,
        # cancel removed, restart on changed cadence) without a daemon restart.
        self._interval_loops: dict[str, tuple[asyncio.Task[None], int | None]] = {}
        self._running = False

    def _track(self, task: asyncio.Task[None]) -> None:
        """Hold a strong reference to *task* and drop it once it completes."""
        self._interval_tasks.append(task)
        task.add_done_callback(
            lambda t: self._interval_tasks.remove(t) if t in self._interval_tasks else None
        )

    def register_streams(self, specs: list[JobSpec]) -> None:
        """Register stream workers from config without starting them.

        Called by the app lifespan so stream jobs appear in ``/api/streams``
        and can be started/stopped from the UI even in ``dccd ui`` mode.
        """
        for spec in specs:
            if spec.operation == "stream" and spec.enabled and spec.id not in self._streams:
                worker = _StreamWorker(
                    spec, self._registry, self._store, self._runs_store, self._events
                )
                self._streams[spec.id] = worker

    async def sync_streams(self, specs: list[JobSpec]) -> None:
        """Reconcile registered stream workers with *specs*.

        Registers workers for new stream specs and **stops + drops** workers
        whose spec is no longer present (e.g. a job deleted from the UI). Without
        the drop, a deleted stream would keep running and stay controllable via
        ``/api/streams`` (its config entry is already gone).
        """
        wanted = {s.id for s in specs if s.operation == "stream" and s.enabled}
        self.register_streams(specs)
        for sid in list(self._streams):
            if sid not in wanted:
                await self._streams[sid].stop()
                del self._streams[sid]

    def _interval_of(self, spec: JobSpec) -> int:
        """Resolve the recurring interval (seconds) for a backfill spec."""
        return spec.trigger.every or spec.target.span or 3600

    async def sync_intervals(self, specs: list[JobSpec]) -> None:
        """Reconcile recurring backfill loops with *specs*.

        Starts a loop for each newly-scheduled ``interval``/``cron`` backfill
        job, cancels loops whose schedule was removed (now ``manual``/deleted),
        and restarts a loop whose cadence changed. No-op unless the scheduler is
        running (``dccd start``); in ``dccd ui`` mode schedules just persist to
        config and take effect on the next daemon start.
        """
        if not self._running:
            return
        wanted = {
            s.id: s for s in specs
            if s.operation == "backfill" and s.enabled
            and s.trigger.kind in ("interval", "cron")
        }
        for sid in list(self._interval_loops):
            task, every = self._interval_loops[sid]
            spec = wanted.get(sid)
            if spec is None or task.done() or self._interval_of(spec) != every:
                task.cancel()
                del self._interval_loops[sid]
        for sid, spec in wanted.items():
            if sid not in self._interval_loops:
                self._interval_loops[sid] = (
                    asyncio.create_task(self._interval_loop(spec)),
                    self._interval_of(spec),
                )

    async def run_now(self, spec: JobSpec) -> None:
        """Trigger a one-shot backfill for *spec* immediately."""
        self._track(asyncio.create_task(self._run_once(spec)))

    async def start(self, specs: list[JobSpec]) -> None:
        """Start all enabled specs (full daemon mode)."""
        self._running = True
        for spec in specs:
            if not spec.enabled:
                continue
            if spec.trigger.kind == "supervised":
                if spec.id not in self._streams:
                    worker = _StreamWorker(spec, self._registry, self._store, self._runs_store, self._events)
                    self._streams[spec.id] = worker
                self._streams[spec.id].start()
            elif spec.trigger.kind in ("interval", "cron"):
                self._interval_loops[spec.id] = (
                    asyncio.create_task(self._interval_loop(spec)),
                    self._interval_of(spec),
                )
            elif spec.trigger.kind == "once":
                # Track the task so stop() can cancel it and the event loop
                # cannot silently GC it before it finishes.
                self._track(asyncio.create_task(self._run_once(spec)))

    async def stop(self) -> None:
        """Stop all running jobs."""
        self._running = False
        for task in self._interval_tasks:
            task.cancel()
        for task, _ in self._interval_loops.values():
            task.cancel()
        for worker in self._streams.values():
            await worker.stop()
        self._interval_tasks.clear()
        self._interval_loops.clear()

    async def _interval_loop(self, spec: JobSpec) -> None:
        every = spec.trigger.every or spec.target.span or 3600
        while self._running:
            await self._run_once(spec)
            await asyncio.sleep(every)

    async def _run_once(self, spec: JobSpec) -> None:
        from dccd.application.operations import backfill
        try:
            run_events = self._events.for_run(f"{spec.id}@{int(time.time())}")
            await backfill(
                spec,
                registry=self._registry,
                store=self._store,
                runs_store=self._runs_store,
                events=run_events,
            )
        except Exception as exc:
            logger.error("Scheduled job %s failed: %s", spec.id, exc)

    def start_stream(self, spec_id: str) -> bool:
        """Start one registered stream by spec id; return whether it existed."""
        worker = self._streams.get(spec_id)
        if worker:
            worker.start()
            return True
        return False

    async def stop_stream(self, spec_id: str) -> bool:
        """Stop one registered stream by spec id; return whether it existed."""
        worker = self._streams.get(spec_id)
        if worker:
            await worker.stop()
            return True
        return False

    def stream_status(self) -> dict[str, bool]:
        """Map of stream spec id to running state."""
        return {sid: w.is_running for sid, w in self._streams.items()}
