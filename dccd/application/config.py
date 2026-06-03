"""Application configuration — extends daemon config with v3 JobSpecs."""

from __future__ import annotations

import os
import pathlib

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
    {"binance", "kraken", "bybit", "okx", "coinbase", "bitfinex", "bitmex"}
)


class SettingsConfig(BaseModel):
    data_path: str = "./data/crypto"
    timezone: str = "local"
    ui_host: str = "127.0.0.1"
    ui_port: int = 8080
    ui_auth_token: str | None = None
    ui_allow_origins: list[str] = Field(default_factory=list)

    @field_validator("data_path")
    @classmethod
    def _expand(cls, v: str) -> str:
        return str(pathlib.Path(v).expanduser())

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
    provider: str = "rclone"
    remote: str


class StorageConfig(BaseModel):
    local_path: str = ""
    remotes: list[RemoteConfig] = Field(default_factory=list)
    sync_interval: int = 3600


class AlertConfig(BaseModel):
    webhook_url: str | None = None
    max_consecutive_errors: int = 3


class JobConfig(BaseModel):
    """YAML job definition — more human-friendly than JobSpec.

    Validation enforces:
    - ``exchange`` must be a known exchange name.
    - ``data_type='ohlc'`` requires ``span`` to be set.
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
    def _validate_span_for_ohlc(self) -> "JobConfig":
        if self.data_type == "ohlc" and self.span is None:
            raise ValueError("'span' is required when data_type='ohlc'")
        return self

    def to_job_specs(self) -> list[JobSpec]:
        """Expand a multi-pair JobConfig into a list of JobSpecs."""
        specs = []
        data_type = DataType(self.data_type)
        trigger = Trigger(
            kind=self.trigger_kind,  # type: ignore[arg-type]
            every=self.every or self.span,
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
                start=self.start,  # type: ignore[arg-type]
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
        specs = []
        for job in self.jobs:
            specs.extend(job.to_job_specs())
        return specs


def resolve_config_path(path: str | pathlib.Path | None = None) -> pathlib.Path:
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
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    return AppConfig.model_validate(data)
