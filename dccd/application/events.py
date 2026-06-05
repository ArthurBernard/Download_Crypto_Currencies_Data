"""Event bus — Progress, Log, Status events."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any, Literal

from pydantic import BaseModel

__all__ = [
    "Event",
    "ProgressEvent",
    "LogEvent",
    "StatusEvent",
    "StreamSampleEvent",
    "EventBus",
]

logger = logging.getLogger(__name__)


class ProgressEvent(BaseModel):
    """Progress of a run (windows or time covered)."""
    kind: Literal["progress"] = "progress"
    run_id: str
    done: int
    total: int
    unit: str = "windows"


class LogEvent(BaseModel):
    """A log line emitted by a run."""
    kind: Literal["log"] = "log"
    run_id: str
    level: str = "info"
    message: str


class StatusEvent(BaseModel):
    """A run state change (running, succeeded, failed, …)."""
    kind: Literal["status"] = "status"
    run_id: str
    state: str


class StreamSampleEvent(BaseModel):
    """A liveness sample from a running stream (last trade/price/quote).

    Emitted (throttled) by :func:`dccd.application.operations.stream` so the
    Live UI can prove a stream is actually receiving data, without persisting
    the sample. ``ts`` is the record timestamp (nanoseconds UTC); ``label`` is
    a short human string (e.g. ``"42153.7"`` or ``"bid 42150 / ask 42151"``).
    """
    kind: Literal["sample"] = "sample"
    run_id: str
    ts: int
    label: str


Event = ProgressEvent | LogEvent | StatusEvent | StreamSampleEvent

Handler = Callable[[Event], Any]


class EventBus:
    """Pub-sub bus carrying operation events to interested subscribers.

    Operations emit :class:`ProgressEvent`, :class:`LogEvent` and
    :class:`StatusEvent` (tagged by ``run_id``). Subscribers — the HTTP API's
    SSE endpoint, the health monitor — receive them either by registering a
    handler or by draining an internal queue (:meth:`enable_queue`). Use
    :meth:`for_run` to get a small emitter bound to one ``run_id``.

    Examples
    --------
    >>> bus = EventBus()
    >>> seen = []
    >>> bus.subscribe(seen.append)
    >>> bus.for_run('r1').log('started')
    >>> seen[0].message
    'started'
    """

    def __init__(self) -> None:
        self._handlers: list[Handler] = []
        # A *set* of queues so several SSE consumers (Live + Logs + Dashboard
        # in separate tabs) each receive every event. A single shared queue
        # would let the last connection steal events from the others.
        self._queues: set[asyncio.Queue[Event]] = set()

    def subscribe(self, handler: Handler) -> None:
        """Register a handler called for every published event."""
        self._handlers.append(handler)

    def unsubscribe(self, handler: Handler) -> None:
        """Remove a previously registered handler."""
        self._handlers = [h for h in self._handlers if h != handler]

    def emit(self, event: Event) -> None:
        """Publish an event to all handlers and every registered queue."""
        for handler in self._handlers:
            try:
                handler(event)
            except Exception:
                logger.exception("EventBus handler error")
        for queue in self._queues:
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                pass

    def add_queue(self, maxsize: int = 1000) -> asyncio.Queue[Event]:
        """Register and return a new queue that receives every event (for SSE)."""
        queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=maxsize)
        self._queues.add(queue)
        return queue

    def remove_queue(self, queue: asyncio.Queue[Event]) -> None:
        """Unregister a queue (call on SSE disconnect)."""
        self._queues.discard(queue)

    def enable_queue(self, maxsize: int = 1000) -> asyncio.Queue[Event]:
        """Backwards-compatible alias for :meth:`add_queue`."""
        return self.add_queue(maxsize)

    def for_run(self, run_id: str) -> "RunEvents":
        """Return a small emitter bound to *run_id* (``.progress/.log/.status``)."""
        return RunEvents(self, run_id)


class RunEvents:
    """Scoped event emitter for a single run."""

    def __init__(self, bus: EventBus, run_id: str) -> None:
        self._bus = bus
        self._run_id = run_id

    def progress(self, done: int, total: int, unit: str = "windows") -> None:
        """Emit a :class:`ProgressEvent` for this run."""
        self._bus.emit(ProgressEvent(run_id=self._run_id, done=done, total=total, unit=unit))

    def log(self, msg: str, level: str = "info") -> None:
        """Emit a :class:`LogEvent` for this run."""
        self._bus.emit(LogEvent(run_id=self._run_id, level=level, message=msg))

    def status(self, state: str) -> None:
        """Emit a :class:`StatusEvent` for this run."""
        self._bus.emit(StatusEvent(run_id=self._run_id, state=state))

    def sample(self, ts: int, label: str) -> None:
        """Emit a :class:`StreamSampleEvent` (stream liveness) for this run."""
        self._bus.emit(StreamSampleEvent(run_id=self._run_id, ts=ts, label=label))
