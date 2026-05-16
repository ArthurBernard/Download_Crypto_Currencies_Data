#!/usr/bin/env python3
# coding: utf-8

""" Declarative configuration for the dccd daemon.

Loads a YAML file and validates it with Pydantic v2 models.

"""

from __future__ import annotations

import pathlib
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

__all__ = [
    'CollectorConfig',
    'AlertConfig',
    'HistoJob',
    'RemoteConfig',
    'StorageConfig',
    'StreamJob',
    'load_config',
]

SUPPORTED_HISTO_EXCHANGES: frozenset[str] = frozenset(
    {'binance', 'kraken', 'bybit', 'okx', 'coinbase'}
)
SUPPORTED_STREAM_EXCHANGES: frozenset[str] = frozenset(
    {'binance', 'kraken', 'bybit', 'okx', 'bitfinex', 'bitmex'}
)
SUPPORTED_FORMATS: frozenset[str] = frozenset({'xlsx', 'csv', 'parquet'})
SUPPORTED_BY_PERIOD: frozenset[str] = frozenset({'Y', 'M', 'D'})


class RemoteConfig(BaseModel):
    """ Remote storage configuration for rclone.

    Parameters
    ----------
    provider : str
        Remote provider, default is ``'rclone'``.
    remote : str
        rclone remote destination, e.g. ``'mynas:crypto/'``.

    """

    provider: str = 'rclone'
    remote: str


class StorageConfig(BaseModel):
    """ Local and optional remote storage configuration.

    Parameters
    ----------
    local_path : str
        Absolute path where data is stored on the daemon host.
    remotes : list of RemoteConfig
        Remote destinations.  Empty list (default) keeps data locally only.
        Multiple entries are all synced by :class:`SyncService`.
    sync_interval : int
        Seconds between periodic syncs to remote destinations.  ``0`` disables
        the sync service.  Default is ``3600`` (1 hour).

    """

    local_path: str
    remotes: list[RemoteConfig] = Field(default_factory=list)
    sync_interval: int = 3600


class HistoJob(BaseModel):
    """ Historical (REST) data collection job.

    Parameters
    ----------
    exchange : str
        Exchange name. Must be one of ``SUPPORTED_HISTO_EXCHANGES``.
    pairs : list of str
        Trading pairs in ``'CRYPTO/FIAT'`` format (e.g. ``'BTC/USDT'``).
    span : int
        Candle interval in seconds. Must be >= 60.
    format : str
        Output format: ``'xlsx'``, ``'csv'``, or ``'parquet'``.
    by_period : str
        File grouping period: ``'Y'`` (year), ``'M'`` (month), ``'D'`` (day).

    """

    exchange: str
    pairs: list[str]
    span: int
    format: str = 'parquet'
    by_period: str = 'Y'

    @field_validator('exchange')
    @classmethod
    def _validate_exchange(cls, v: str) -> str:
        if v not in SUPPORTED_HISTO_EXCHANGES:
            raise ValueError(
                f"Unknown exchange {v!r}. "
                f"Supported: {sorted(SUPPORTED_HISTO_EXCHANGES)}"
            )
        return v

    @field_validator('pairs')
    @classmethod
    def _validate_pairs(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("'pairs' must not be empty")
        for pair in v:
            if '/' not in pair:
                raise ValueError(
                    f"Pair {pair!r} must be in 'CRYPTO/FIAT' format (e.g. 'BTC/USDT')"
                )
        return v

    @field_validator('span')
    @classmethod
    def _validate_span(cls, v: int) -> int:
        if v < 60:
            raise ValueError(f"span must be >= 60 seconds, got {v}")
        return v

    @field_validator('format')
    @classmethod
    def _validate_format(cls, v: str) -> str:
        if v not in SUPPORTED_FORMATS:
            raise ValueError(
                f"Unknown format {v!r}. Supported: {sorted(SUPPORTED_FORMATS)}"
            )
        return v

    @field_validator('by_period')
    @classmethod
    def _validate_by_period(cls, v: str) -> str:
        if v not in SUPPORTED_BY_PERIOD:
            raise ValueError(
                f"Unknown by_period {v!r}. Supported: {sorted(SUPPORTED_BY_PERIOD)}"
            )
        return v


class StreamJob(BaseModel):
    """ Real-time (WebSocket) data collection job.

    Parameters
    ----------
    exchange : str
        Exchange name. Must be one of ``SUPPORTED_STREAM_EXCHANGES``.
    pairs : list of str
        Trading pairs (format depends on exchange).
    channels : list of str
        WebSocket channels to subscribe to (e.g. ``['trades', 'book']``).
    time_step : int
        Snapshot interval in seconds, default is 60.

    """

    exchange: str
    pairs: list[str]
    channels: list[str]
    time_step: int = 60

    @field_validator('exchange')
    @classmethod
    def _validate_exchange(cls, v: str) -> str:
        if v not in SUPPORTED_STREAM_EXCHANGES:
            raise ValueError(
                f"Unknown exchange {v!r}. "
                f"Supported: {sorted(SUPPORTED_STREAM_EXCHANGES)}"
            )
        return v

    @field_validator('pairs', 'channels')
    @classmethod
    def _validate_nonempty(cls, v: list[str], info: Any) -> list[str]:
        if not v:
            raise ValueError(f"'{info.field_name}' must not be empty")
        return v


class AlertConfig(BaseModel):
    """ Optional alerting configuration.

    Parameters
    ----------
    webhook_url : str or None
        Slack/Discord webhook URL for error notifications. ``None`` disables alerts.
    max_consecutive_errors : int
        Number of consecutive job failures before sending an alert, default 3.

    """

    webhook_url: str | None = None
    max_consecutive_errors: int = 3


class CollectorConfig(BaseModel):
    """ Root configuration model for the dccd daemon.

    Parameters
    ----------
    storage : StorageConfig
        Local (and optional remote) storage settings.
    histo_jobs : list of HistoJob
        REST API polling jobs.
    stream_jobs : list of StreamJob
        WebSocket streaming jobs.
    alerts : AlertConfig
        Alerting settings.

    """

    storage: StorageConfig
    histo_jobs: list[HistoJob] = Field(default_factory=list)
    stream_jobs: list[StreamJob] = Field(default_factory=list)
    alerts: AlertConfig = Field(default_factory=AlertConfig)

    @model_validator(mode='after')
    def _at_least_one_job(self) -> 'CollectorConfig':
        if not self.histo_jobs and not self.stream_jobs:
            raise ValueError(
                "Configuration must define at least one job "
                "(histo_jobs or stream_jobs)"
            )
        return self


def load_config(path: str | pathlib.Path) -> CollectorConfig:
    """ Load and validate a YAML daemon configuration file.

    Parameters
    ----------
    path : str or pathlib.Path
        Path to the YAML configuration file.

    Returns
    -------
    CollectorConfig
        Validated configuration object.

    Raises
    ------
    FileNotFoundError
        If *path* does not exist.
    yaml.YAMLError
        If the file contains invalid YAML.
    pydantic.ValidationError
        If the configuration fails validation.

    """
    with open(path) as f:
        data = yaml.safe_load(f)
    return CollectorConfig.model_validate(data)
