"""Job model — JobSpec, JobRun, Trigger, JobParams."""

from __future__ import annotations

from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel

from dccd.domain.symbol import Symbol
from dccd.domain.types import DataType

__all__ = ["Trigger", "JobTarget", "JobParams", "JobSpec", "JobRun", "RunState"]


class RunState(str, Enum):
    """Lifecycle state of a job run."""
    PENDING = "pending"
    RUNNING = "running"
    RECONNECTING = "reconnecting"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class Trigger(BaseModel, frozen=True):
    """Job trigger — when/how to execute.

    ``manual`` jobs are never auto-run by the scheduler; they exist only to be
    triggered on demand (the UI Run button / ``POST /api/jobs/run``).
    """

    kind: Literal["once", "interval", "cron", "supervised", "manual"]
    at: int | None = None
    every: int | None = None
    cron: str | None = None


class JobTarget(BaseModel, frozen=True):
    """What to collect — exchange + symbol + data type."""

    exchange: str
    symbol: Symbol
    data_type: DataType
    span: int | None = None


class JobParams(BaseModel):
    """Operation parameters (all optional, per-operation)."""

    # ``start`` accepts the sentinels "last"/"origin", an ISO date string
    # ("2024-01-01"), or a nanosecond integer — all parsed in operations.backfill.
    start: int | str = "last"
    depth: int | None = None
    snapshot_interval: int | None = None
    derive_from: DataType | None = None
    transport: Literal["rest", "ws"] | None = None


class JobSpec(BaseModel):
    """Declarative job definition.

    A histo job is: backfill + interval trigger (+ start="last").
    A stream job is: stream + supervised trigger.
    """

    id: str
    operation: Literal["backfill", "stream"]
    target: JobTarget
    trigger: Trigger
    params: JobParams = JobParams()
    enabled: bool = True
    origin: Literal["config", "runtime"] = "config"

    @classmethod
    def make_id(cls, operation: str, target: JobTarget) -> str:
        """Build a stable job id from the operation and target."""
        parts = [operation, target.exchange, str(target.symbol), target.data_type.value]
        if target.span:
            parts.append(f"{target.span}s")
        return ":".join(parts)


class JobRun(BaseModel):
    """One execution of a JobSpec."""

    run_id: str
    spec_id: str
    operation: str
    target: JobTarget
    state: RunState = RunState.PENDING
    started_at: int | None = None
    ended_at: int | None = None
    rows_written: int = 0
    error: str | None = None
    progress: dict[str, Any] | None = None
    log_tail: list[str] = []
