#!/usr/bin/env python3
# coding: utf-8

import pathlib

import pytest
import yaml
from pydantic import ValidationError

from dccd.daemon.config import (
    DEFAULT_CONFIG_PATH,
    CollectorConfig,
    load_config,
    resolve_config_path,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_VALID_STORAGE = {'local_path': '/data/crypto/'}

_VALID_HISTO_JOB = {
    'exchange': 'binance',
    'pairs': ['BTC/USDT', 'ETH/USDT'],
    'span': 3600,
    'format': 'parquet',
}

_VALID_CONFIG = {
    'storage': _VALID_STORAGE,
    'histo_jobs': [_VALID_HISTO_JOB],
}


def _make_config_file(tmp_path: pathlib.Path, data: dict) -> pathlib.Path:
    p = tmp_path / 'config.yml'
    p.write_text(yaml.dump(data))
    return p


# ---------------------------------------------------------------------------
# CollectorConfig — valid cases
# ---------------------------------------------------------------------------

def test_valid_full_config():
    cfg = CollectorConfig.model_validate(_VALID_CONFIG)
    assert cfg.storage.local_path == '/data/crypto/'
    assert len(cfg.histo_jobs) == 1
    assert cfg.histo_jobs[0].exchange == 'binance'
    assert cfg.histo_jobs[0].pairs == ['BTC/USDT', 'ETH/USDT']
    assert cfg.histo_jobs[0].span == 3600


def test_default_values_applied():
    cfg = CollectorConfig.model_validate(_VALID_CONFIG)
    assert cfg.stream_jobs == []
    assert cfg.alerts.webhook_url is None
    assert cfg.alerts.max_consecutive_errors == 3
    assert cfg.histo_jobs[0].format == 'parquet'


def test_remote_config_parsed():
    data = {
        **_VALID_CONFIG,
        'storage': {
            'local_path': '/data/',
            'remotes': [{'provider': 'rclone', 'remote': 'mynas:crypto/'}],
        },
    }
    cfg = CollectorConfig.model_validate(data)
    assert len(cfg.storage.remotes) == 1
    assert cfg.storage.remotes[0].remote == 'mynas:crypto/'


def test_multiple_remotes_parsed():
    data = {
        **_VALID_CONFIG,
        'storage': {
            'local_path': '/data/',
            'remotes': [
                {'provider': 'rclone', 'remote': 'mynas:crypto/'},
                {'provider': 'rclone', 'remote': 's3:bucket/crypto/'},
            ],
        },
    }
    cfg = CollectorConfig.model_validate(data)
    assert len(cfg.storage.remotes) == 2


def test_sync_interval_default():
    cfg = CollectorConfig.model_validate(_VALID_CONFIG)
    assert cfg.storage.sync_interval == 3600


def test_sync_interval_custom():
    data = {**_VALID_CONFIG, 'storage': {**_VALID_STORAGE, 'sync_interval': 300}}
    cfg = CollectorConfig.model_validate(data)
    assert cfg.storage.sync_interval == 300


# ---------------------------------------------------------------------------
# HistoJob — validation errors
# ---------------------------------------------------------------------------

def test_unknown_exchange_raises():
    data = {**_VALID_CONFIG, 'histo_jobs': [{**_VALID_HISTO_JOB, 'exchange': 'poloniex'}]}
    with pytest.raises(ValidationError, match='Unknown exchange'):
        CollectorConfig.model_validate(data)


def test_span_too_small_raises():
    data = {**_VALID_CONFIG, 'histo_jobs': [{**_VALID_HISTO_JOB, 'span': 30}]}
    with pytest.raises(ValidationError, match='span must be >= 60'):
        CollectorConfig.model_validate(data)


def test_unsupported_format_raises():
    data = {**_VALID_CONFIG, 'histo_jobs': [{**_VALID_HISTO_JOB, 'format': 'json'}]}
    with pytest.raises(ValidationError, match='Unknown format'):
        CollectorConfig.model_validate(data)


def test_pair_missing_slash_raises():
    data = {**_VALID_CONFIG, 'histo_jobs': [{**_VALID_HISTO_JOB, 'pairs': ['BTCUSDT']}]}
    with pytest.raises(ValidationError, match="CRYPTO/FIAT"):
        CollectorConfig.model_validate(data)


def test_empty_pairs_raises():
    data = {**_VALID_CONFIG, 'histo_jobs': [{**_VALID_HISTO_JOB, 'pairs': []}]}
    with pytest.raises(ValidationError, match="must not be empty"):
        CollectorConfig.model_validate(data)



def test_no_jobs_raises():
    data = {'storage': _VALID_STORAGE}
    with pytest.raises(ValidationError, match='at least one job'):
        CollectorConfig.model_validate(data)


# ---------------------------------------------------------------------------
# load_config
# ---------------------------------------------------------------------------

def test_load_config_valid(tmp_path):
    p = _make_config_file(tmp_path, _VALID_CONFIG)
    cfg = load_config(p)
    assert isinstance(cfg, CollectorConfig)
    assert cfg.histo_jobs[0].exchange == 'binance'


def test_load_config_missing_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_config(tmp_path / 'nonexistent.yml')


def test_load_config_invalid_yaml(tmp_path):
    p = tmp_path / 'bad.yml'
    p.write_text(': invalid: yaml: {')
    with pytest.raises(yaml.YAMLError):
        load_config(p)


def test_load_config_validation_error(tmp_path):
    bad = {**_VALID_CONFIG, 'histo_jobs': [{**_VALID_HISTO_JOB, 'span': 10}]}
    p = _make_config_file(tmp_path, bad)
    with pytest.raises(ValidationError):
        load_config(p)


# ---------------------------------------------------------------------------
# resolve_config_path
# ---------------------------------------------------------------------------

def test_resolve_explicit_path(tmp_path):
    p = tmp_path / 'my.yml'
    result = resolve_config_path(str(p))
    assert result == p


def test_resolve_explicit_path_expanduser(tmp_path, monkeypatch):
    monkeypatch.setenv('HOME', str(tmp_path))
    result = resolve_config_path('~/my.yml')
    assert result == tmp_path / 'my.yml'


def test_resolve_cwd_fallback(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    cwd_cfg = tmp_path / 'config.yml'
    cwd_cfg.write_text(yaml.dump(_VALID_CONFIG))
    result = resolve_config_path(None)
    assert result == pathlib.Path('config.yml')


def test_resolve_xdg_fallback(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    xdg_dir = tmp_path / 'xdg'
    xdg_cfg = xdg_dir / 'dccd' / 'config.yml'
    xdg_cfg.parent.mkdir(parents=True)
    xdg_cfg.write_text(yaml.dump(_VALID_CONFIG))
    monkeypatch.setenv('XDG_CONFIG_HOME', str(xdg_dir))
    result = resolve_config_path(None)
    assert result == xdg_cfg


def test_resolve_no_config_raises(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path / 'empty_xdg'))
    with pytest.raises(FileNotFoundError, match='No config file found'):
        resolve_config_path(None)


def test_default_config_path_constant():
    assert DEFAULT_CONFIG_PATH.parts[-2:] == ('dccd', 'config.yml')
