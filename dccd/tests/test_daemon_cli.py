#!/usr/bin/env python3
# coding: utf-8

"""Tests for dccd.daemon.cli."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from typer.testing import CliRunner

from dccd.daemon.cli import app

runner = CliRunner()

_MINIMAL_CONFIG = {
    'storage': {'local_path': '/tmp/dccd_test_data'},
    'histo_jobs': [
        {'exchange': 'binance', 'pairs': ['BTC/USDT'], 'span': 3600},
    ],
}


@pytest.fixture()
def config_file(tmp_path: Path) -> Path:
    p = tmp_path / 'config.yml'
    p.write_text(yaml.dump(_MINIMAL_CONFIG))
    return p


def test_validate_ok(config_file: Path) -> None:
    result = runner.invoke(app, ['validate', '--config', str(config_file)])
    assert result.exit_code == 0
    assert 'valid' in result.output.lower()
    assert 'binance' not in result.output  # summary is counts, not exchange names


def test_validate_missing_file(tmp_path: Path) -> None:
    result = runner.invoke(app, ['validate', '--config', str(tmp_path / 'no.yml')])
    assert result.exit_code == 1


def test_validate_bad_config(tmp_path: Path) -> None:
    bad = tmp_path / 'bad.yml'
    bad.write_text(yaml.dump({'storage': {'local_path': '/tmp'}, 'histo_jobs': []}))
    result = runner.invoke(app, ['validate', '--config', str(bad)])
    assert result.exit_code == 1


def test_run_calls_run_once(config_file: Path) -> None:
    with patch('dccd.daemon.health.HealthMonitor') as MockHealth, \
         patch('dccd.daemon.scheduler.run_once') as mock_run_once:
        MockHealth.return_value.get_metrics.return_value = {}
        result = runner.invoke(app, ['run', '--config', str(config_file)])
    assert result.exit_code == 0
    mock_run_once.assert_called_once()


def test_status_no_metrics(config_file: Path) -> None:
    result = runner.invoke(app, ['status', '--config', str(config_file)])
    assert result.exit_code == 0
    assert 'No metrics yet' in result.output


def test_status_shows_table(config_file: Path, tmp_path: Path) -> None:
    dccd_dir = tmp_path / '.dccd_test_storage' / '.dccd'
    dccd_dir.mkdir(parents=True)
    metrics = {
        'binance/BTC/USDT': {
            'last_run_at': 1747440000.0,
            'last_success_at': 1747440000.0,
            'rows_collected': 100,
            'errors_count': 0,
        }
    }
    (dccd_dir / 'metrics.json').write_text(json.dumps(metrics))

    cfg = {
        'storage': {'local_path': str(dccd_dir.parent)},
        'histo_jobs': [{'exchange': 'binance', 'pairs': ['BTC/USDT'], 'span': 3600}],
    }
    cfg_file = tmp_path / 'cfg2.yml'
    cfg_file.write_text(yaml.dump(cfg))

    result = runner.invoke(app, ['status', '--config', str(cfg_file)])
    assert result.exit_code == 0
    assert 'binance/BTC/USDT' in result.output
    assert '100' in result.output


def test_add_histo_job(config_file: Path) -> None:
    result = runner.invoke(app, [
        'add',
        '--exchange', 'kraken',
        '--pair', 'ETH/USD',
        '--span', '86400',
        '--config', str(config_file),
    ])
    assert result.exit_code == 0
    loaded = yaml.safe_load(config_file.read_text())
    pairs_all = [
        p for job in loaded['histo_jobs'] for p in job['pairs']
    ]
    assert 'ETH/USD' in pairs_all
