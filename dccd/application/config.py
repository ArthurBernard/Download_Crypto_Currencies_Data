"""Application configuration — extends daemon config with v3 JobSpecs."""

from __future__ import annotations

import os
import pathlib
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

from dccd.application.jobs import JobParams, JobSpec, JobTarget, Trigger
from dccd.domain.symbol import Symbol
from dccd.domain.types import DataType

__all__ = [
    "AlertConfig",
    "AppConfig",
    "JobConfig",
    "RemoteConfig",
    "SettingsConfig",
    "StorageConfig",
    "load_config",
    "resolve_config_path",
    "DEFAULT_CONFIG_PATH",
]

_XDG_CONFIG_HOME = pathlib.Path(
    os.environ.get("XDG_CONFIG_HOME", "~/.config")
).expanduser()

DEFAULT_CONFIG_PATH = _XDG_CONFIG_HOME / "dccd" / "config.yml"

SUPPORTED_EXCHANGES: frozenset[str] = frozenset(
    {"binance", "kraken", "krakenfutures", "bybit", "okx", "coinbase",
     "bitfinex", "bitmex"}
)


class SettingsConfig(BaseModel):
    """Global settings: data path, timezone, and web-UI bind/auth.

    Hardening knobs (all off by default, i.e. localhost-safe):

    - ``ui_readonly`` — block mutating HTTP methods on ``/api/*`` (read-only share).
    - ``ui_rate_limit`` — requests/sec per client on ``/api/*`` (``0`` disables).
    - ``ui_trusted_proxy`` — trust ``X-Forwarded-For`` as the rate-limit client key.
      Enable **only** behind a reverse proxy that overwrites the header, else a direct
      client can forge it and bypass the limit.
    - ``runs_retention_days`` — delete terminal non-failed runs (``succeeded``,
      ``stale``, ``cancelled``) older than this many days at daemon boot.  ``0``
      disables the sweep (rows accumulate indefinitely).  Default ``90``.
    """
    data_path: str = "./data/crypto"
    timezone: str = "local"
    ui_host: str = "127.0.0.1"
    ui_port: int = 8080
    ui_auth_token: str | None = None
    ui_allow_origins: list[str] = Field(default_factory=list)
    ui_readonly: bool = False
    ui_rate_limit: int = 0
    ui_trusted_proxy: bool = False
    runs_retention_days: int = 90

    @field_validator("data_path")
    @classmethod
    def _expand(cls, v: str) -> str:
        return str(pathlib.Path(v).expanduser())

    @field_validator("ui_rate_limit")
    @classmethod
    def _non_negative(cls, v: int) -> int:
        if v < 0:
            raise ValueError("ui_rate_limit must be >= 0")
        return v

    @field_validator("runs_retention_days")
    @classmethod
    def _non_negative_retention(cls, v: int) -> int:
        if v < 0:
            raise ValueError("runs_retention_days must be >= 0")
        return v

    @field_validator("timezone")
    @classmethod
    def _validate_tz(cls, v: str) -> str:
        if v.upper() in ("LOCAL", "UTC"):
            return v
        try:
            from zoneinfo import ZoneInfo
            ZoneInfo(v)
        except KeyError:
            raise ValueError(f"Unknown timezone {v!r}")
        return v


class RemoteConfig(BaseModel):
    """One rclone remote target for sync."""
    provider: str = "rclone"
    remote: str


class StorageConfig(BaseModel):
    """Storage settings: local path, rclone remotes, sync interval, and the
    free-space floor (GiB) that triggers purging already-synced local files."""
    local_path: str = ""
    remotes: list[RemoteConfig] = Field(default_factory=list)
    sync_interval: int = 3600
    min_free_gb: float = 0.0


class AlertConfig(BaseModel):
    """Health-alert settings: webhook URL and error threshold."""
    webhook_url: str | None = None
    max_consecutive_errors: int = 3


class JobConfig(BaseModel):
    """YAML job definition — more human-friendly than JobSpec.

    Validation enforces:
    - ``exchange`` must be a known exchange name.
    - ``data_type='ohlc'`` or ``'open_interest'`` requires ``span`` to be set.
    - ``pairs`` must be non-empty and use ``BASE/QUOTE`` format.
    """

    exchange: str
    pairs: list[str]
    data_type: str = "ohlc"
    operation: str = "backfill"
    span: int | None = None
    trigger_kind: str = "interval"
    every: int | None = None
    cron: str | None = None
    start: str = "last"
    depth: int | None = None
    snapshot_interval: int | None = None

    @field_validator("exchange")
    @classmethod
    def _validate_exchange(cls, v: str) -> str:
        if v.lower() not in SUPPORTED_EXCHANGES:
            raise ValueError(
                f"Unknown exchange {v!r}. Supported: {sorted(SUPPORTED_EXCHANGES)}"
            )
        return v.lower()

    @field_validator("pairs")
    @classmethod
    def _validate_pairs(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("'pairs' must not be empty")
        for pair in v:
            if "/" not in pair and "-" not in pair:
                raise ValueError(
                    f"Pair {pair!r} must use 'BASE/QUOTE' or 'BASE-QUOTE' format"
                )
        return v

    @model_validator(mode="after")
    def _validate_span_required(self) -> "JobConfig":
        if self.data_type in ("ohlc", "open_interest") and self.span is None:
            raise ValueError(
                "'span' is required when data_type='ohlc' or 'open_interest'"
            )
        return self

    def to_job_specs(self) -> list[JobSpec]:
        """Expand a multi-pair JobConfig into a list of JobSpecs."""
        specs = []
        data_type = DataType(self.data_type)
        # Only recurring triggers carry an interval; for ``interval``/``cron`` it
        # defaults to the span when unset. ``manual``/``once``/``supervised``
        # must not report a spurious cadence (the UI reads it as "scheduled").
        every = (
            (self.every or self.span)
            if self.trigger_kind in ("interval", "cron")
            else self.every
        )
        trigger = Trigger(
            kind=self.trigger_kind,  # type: ignore[arg-type]
            every=every,
            cron=self.cron,
        )
        for pair in self.pairs:
            sym = Symbol.parse(pair)
            target = JobTarget(
                exchange=self.exchange,
                symbol=sym,
                data_type=data_type,
                span=self.span,
            )
            params = JobParams(
                start=self.start,
                depth=self.depth,
                snapshot_interval=self.snapshot_interval,
            )
            spec = JobSpec(
                id=JobSpec.make_id(self.operation, target),
                operation=self.operation,  # type: ignore[arg-type]
                target=target,
                trigger=trigger,
                params=params,
            )
            specs.append(spec)
        return specs


class AppConfig(BaseModel):
    """Top-level config: settings, storage, alerts and the list of jobs."""
    settings: SettingsConfig = Field(default_factory=SettingsConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    alerts: AlertConfig = Field(default_factory=AlertConfig)
    jobs: list[JobConfig] = Field(default_factory=list)

    @model_validator(mode="after")
    def _propagate_path(self) -> "AppConfig":
        if not self.storage.local_path:
            self.storage.local_path = self.settings.data_path
        return self

    def all_job_specs(self) -> list[JobSpec]:
        """Expand every :class:`JobConfig` into its per-pair :class:`JobSpec` list."""
        specs = []
        for job in self.jobs:
            specs.extend(job.to_job_specs())
        return specs

    # ------------------------------------------------------------------
    # Runtime CRUD — used by the UI/API to add, edit and remove jobs
    # without hand-editing config.yml. Writes normalise to one pair per
    # ``JobConfig`` so a single dataset can be edited/deleted in isolation;
    # multi-pair JobConfigs already in the file are still read normally and
    # are split on demand (see :meth:`update_job_start`/:meth:`remove_job`).
    # ------------------------------------------------------------------

    def _spec_id_of(self, job: "JobConfig", pair: str) -> str:
        """Return the :class:`JobSpec` id a (job, pair) pair would produce."""
        sym = Symbol.parse(pair)
        target = JobTarget(
            exchange=job.exchange,
            symbol=sym,
            data_type=DataType(job.data_type),
            span=job.span,
        )
        return JobSpec.make_id(job.operation, target)

    def add_job(
        self,
        *,
        operation: str,
        exchange: str,
        pair: str,
        data_type: str = "ohlc",
        span: int | None = None,
        start: str = "last",
        trigger_kind: str = "interval",
        every: int | None = None,
        cron: str | None = None,
        depth: int | None = None,
        snapshot_interval: int | None = None,
    ) -> str:
        """Append a single-pair :class:`JobConfig`; return its job id.

        Raises ``ValueError`` if a job with the same id already exists or if
        the field combination is invalid (validation is delegated to
        :class:`JobConfig`).
        """
        job = JobConfig(
            exchange=exchange,
            pairs=[pair],
            data_type=data_type,
            operation=operation,
            span=span,
            trigger_kind=trigger_kind,
            every=every,
            cron=cron,
            start=start,
            depth=depth,
            snapshot_interval=snapshot_interval,
        )
        job_id = self._spec_id_of(job, job.pairs[0])
        existing = {s.id for s in self.all_job_specs()}
        if job_id in existing:
            raise ValueError(f"Job {job_id!r} already exists")
        self.jobs.append(job)
        return job_id

    def remove_job(self, job_id: str) -> bool:
        """Remove the pair matching *job_id*; drop the JobConfig if it empties.

        Returns ``True`` if a matching job was found and removed.
        """
        found = False
        new_jobs: list[JobConfig] = []
        for job in self.jobs:
            remaining = [p for p in job.pairs if self._spec_id_of(job, p) != job_id]
            if len(remaining) != len(job.pairs):
                found = True
                if remaining:
                    new_jobs.append(job.model_copy(update={"pairs": remaining}))
                # else: drop the now-empty JobConfig entirely
            else:
                new_jobs.append(job)
        if found:
            self.jobs = new_jobs
        return found

    def update_job_start(self, job_id: str, start: str) -> bool:
        """Set the ``start`` (first date) of the job matching *job_id*.

        Multi-pair JobConfigs are split so only the targeted pair changes.
        Returns ``True`` if a matching job was found and updated.
        """
        return self._update_job_fields(job_id, {"start": start})

    def update_job_schedule(self, job_id: str, every: int | None) -> bool:
        """Set the recurring backfill schedule of the job matching *job_id*.

        ``every`` is the interval in seconds between automatic backfills (run by
        the daemon's :class:`~dccd.application.scheduler.Scheduler` in
        ``dccd start``); ``None`` clears the schedule (manual runs only). The
        frequency is independent of the OHLC span but must not be smaller than
        it — sampling more often than the bar size just refetches the same bar.

        Multi-pair JobConfigs are split so only the targeted pair changes.
        Returns ``True`` if a matching job was found and updated.
        """
        if every is not None:
            for job in self.jobs:
                if any(self._spec_id_of(job, p) == job_id for p in job.pairs):
                    if job.span and every < job.span:
                        raise ValueError(
                            f"Schedule interval ({every}s) must be ≥ the span "
                            f"({job.span}s)."
                        )
                    break
        # ``every=None`` clears the schedule → a manual (never auto-run) job.
        kind = "interval" if every is not None else "manual"
        return self._update_job_fields(job_id, {"every": every, "trigger_kind": kind})

    def _update_job_fields(self, job_id: str, fields: dict[str, Any]) -> bool:
        """Apply *fields* to the single pair matching *job_id*, splitting the
        owning multi-pair JobConfig so siblings are untouched."""
        new_jobs: list[JobConfig] = []
        found = False
        for job in self.jobs:
            match = [p for p in job.pairs if self._spec_id_of(job, p) == job_id]
            if not match:
                new_jobs.append(job)
                continue
            found = True
            others = [p for p in job.pairs if self._spec_id_of(job, p) != job_id]
            if others:
                new_jobs.append(job.model_copy(update={"pairs": others}))
            new_jobs.append(job.model_copy(update={"pairs": match, **fields}))
        if found:
            self.jobs = new_jobs
        return found


def resolve_config_path(path: str | pathlib.Path | None = None) -> pathlib.Path:
    """Resolve the config path (explicit, then ``./config.yml``, then XDG)."""
    if path is not None:
        return pathlib.Path(path).expanduser()
    xdg_cfg = (
        pathlib.Path(os.environ.get("XDG_CONFIG_HOME", "~/.config")).expanduser()
        / "dccd" / "config.yml"
    )
    candidates = [pathlib.Path("config.yml"), xdg_cfg]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    tried = ", ".join(str(c) for c in candidates)
    raise FileNotFoundError(f"No config file found. Tried: {tried}")


def load_config(path: str | pathlib.Path) -> AppConfig:
    """Load and validate a YAML config into an :class:`AppConfig`."""
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    return AppConfig.model_validate(data)
