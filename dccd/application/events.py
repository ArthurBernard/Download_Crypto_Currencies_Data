"""Event bus — Progress, Log, Status events."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any, Literal

from pydantic import BaseModel

__all__ = ["Event", "ProgressEvent", "LogEvent", "StatusEvent", "EventBus"]

logger = logging.getLogger(__name__)


class ProgressEvent(BaseModel):
    kind: Literal["progress"] = "progress"
    run_id: str
    done: int
    total: int
    unit: str = "windows"


class LogEvent(BaseModel):
    kind: Literal["log"] = "log"
    run_id: str
    level: str = "info"
    message: str


class StatusEvent(BaseModel):
    kind: Literal["status"] = "status"
    run_id: str
    state: str


Event = ProgressEvent | LogEvent | StatusEvent

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
        self._queue: asyncio.Queue[Event] | None = None

    def subscribe(self, handler: Handler) -> None:
        self._handlers.append(handler)

    def unsubscribe(self, handler: Handler) -> None:
        self._handlers = [h for h in self._handlers if h != handler]

    def emit(self, event: Event) -> None:
        for handler in self._handlers:
            try:
                handler(event)
            except Exception:
                logger.exception("EventBus handler error")
        if self._queue is not None:
            try:
                self._queue.put_nowait(event)
            except asyncio.QueueFull:
                pass

    def enable_queue(self, maxsize: int = 1000) -> asyncio.Queue[Event]:
        self._queue = asyncio.Queue(maxsize=maxsize)
        return self._queue

    def for_run(self, run_id: str) -> "RunEvents":
        return RunEvents(self, run_id)


class RunEvents:
    """Scoped event emitter for a single run."""

    def __init__(self, bus: EventBus, run_id: str) -> None:
        self._bus = bus
        self._run_id = run_id

    def progress(self, done: int, total: int, unit: str = "windows") -> None:
        self._bus.emit(ProgressEvent(run_id=self._run_id, done=done, total=total, unit=unit))

    def log(self, msg: str, level: str = "info") -> None:
        self._bus.emit(LogEvent(run_id=self._run_id, level=level, message=msg))

    def status(self, state: str) -> None:
        self._bus.emit(StatusEvent(run_id=self._run_id, state=state))
