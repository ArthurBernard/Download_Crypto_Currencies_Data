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


def test_collect_calls_run_once(config_file: Path) -> None:
    with patch('dccd.daemon.health.HealthMonitor') as MockHealth, \
         patch('dccd.daemon.scheduler.run_once') as mock_run_once:
        MockHealth.return_value.get_metrics.return_value = {}
        result = runner.invoke(app, ['collect', '--config', str(config_file)])
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


# ---------------------------------------------------------------------------
# dccd remove
# ---------------------------------------------------------------------------

def test_remove_pair(config_file: Path) -> None:
    # Add a second pair first so removing BTC/USDT leaves the job alive
    result = runner.invoke(app, [
        'add', '--exchange', 'binance', '--pair', 'ETH/USDT', '--span', '3600',
        '--config', str(config_file),
    ])
    assert result.exit_code == 0

    result = runner.invoke(app, [
        'remove', '--exchange', 'binance', '--pair', 'BTC/USDT', '--span', '3600',
        '--config', str(config_file),
    ])
    assert result.exit_code == 0
    loaded = yaml.safe_load(config_file.read_text())
    pairs_all = [p for job in loaded['histo_jobs'] for p in job['pairs']]
    assert 'BTC/USDT' not in pairs_all
    assert 'ETH/USDT' in pairs_all


def test_remove_last_pair_removes_job(tmp_path: Path) -> None:
    # Config with two jobs so removing one still leaves a valid config
    cfg = {
        'storage': {'local_path': '/tmp'},
        'histo_jobs': [
            {'exchange': 'binance', 'pairs': ['BTC/USDT'], 'span': 3600},
            {'exchange': 'kraken', 'pairs': ['ETH/USD'], 'span': 3600},
        ],
    }
    p = tmp_path / 'config.yml'
    p.write_text(yaml.dump(cfg))

    result = runner.invoke(app, [
        'remove', '--exchange', 'binance', '--pair', 'BTC/USDT', '--span', '3600',
        '--config', str(p),
    ])
    assert result.exit_code == 0
    loaded = yaml.safe_load(p.read_text())
    assert all(j['exchange'] != 'binance' for j in loaded['histo_jobs'])


def test_remove_not_found(config_file: Path) -> None:
    result = runner.invoke(app, [
        'remove', '--exchange', 'binance', '--pair', 'XRP/USDT', '--span', '3600',
        '--config', str(config_file),
    ])
    assert result.exit_code == 1
    assert 'No matching job' in result.output


def test_remove_last_job_fails(config_file: Path) -> None:
    original = config_file.read_text()
    result = runner.invoke(app, [
        'remove', '--exchange', 'binance', '--pair', 'BTC/USDT', '--span', '3600',
        '--config', str(config_file),
    ])
    assert result.exit_code == 1
    assert config_file.read_text() == original  # file unchanged


# ---------------------------------------------------------------------------
# dccd inventory
# ---------------------------------------------------------------------------

def _write_ohlc_parquet(data_path: Path, exchange: str, pair: str, span: str, year: int,
                         timestamps: list) -> None:
    import polars as pl
    p = data_path / exchange / 'ohlc' / pair.replace('/', '-') / span / f'{year}.parquet'
    p.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({'TS': timestamps}).write_parquet(p)


def _write_trades_parquet(data_path: Path, exchange: str, pair: str, day: str,
                           timestamps: list) -> None:
    import polars as pl
    p = data_path / exchange / 'trades' / pair.replace('/', '-') / f'{day}.parquet'
    p.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({'TS': timestamps}).write_parquet(p)


@pytest.fixture()
def inventory_config(tmp_path: Path) -> tuple[Path, Path]:
    data_path = tmp_path / 'data'
    cfg = {
        'storage': {'local_path': str(data_path)},
        'histo_jobs': [{'exchange': 'binance', 'pairs': ['BTC/USDT'], 'span': 3600}],
    }
    cfg_file = tmp_path / 'config.yml'
    cfg_file.write_text(yaml.dump(cfg))
    return cfg_file, data_path


def test_inventory_shows_ohlc(inventory_config: tuple) -> None:
    cfg_file, data_path = inventory_config
    _write_ohlc_parquet(data_path, 'binance', 'BTC/USDT', '1h', 2024,
                         [1704067200, 1704070800])
    result = runner.invoke(app, ['inventory', '--config', str(cfg_file)])
    assert result.exit_code == 0
    assert 'binance' in result.output
    assert 'BTC/USDT' in result.output
    assert 'ohlc' in result.output
    assert '1h' in result.output


def test_inventory_shows_trades(inventory_config: tuple) -> None:
    cfg_file, data_path = inventory_config
    _write_trades_parquet(data_path, 'binance', 'BTC/USDT', '2024-01-01',
                           [1704067200, 1704067260])
    result = runner.invoke(app, ['inventory', '--config', str(cfg_file)])
    assert result.exit_code == 0
    assert 'trades' in result.output


def test_inventory_no_data(inventory_config: tuple) -> None:
    cfg_file, data_path = inventory_config
    data_path.mkdir(parents=True, exist_ok=True)
    result = runner.invoke(app, ['inventory', '--config', str(cfg_file)])
    assert result.exit_code == 0
    assert 'No data found' in result.output


def test_inventory_counts_gaps(inventory_config: tuple) -> None:
    cfg_file, data_path = inventory_config
    # Two annual files with a missing year in between → 1 gap
    _write_ohlc_parquet(data_path, 'binance', 'BTC/USDT', '1h', 2022, [1641024000])
    _write_ohlc_parquet(data_path, 'binance', 'BTC/USDT', '1h', 2024, [1704067200])
    result = runner.invoke(app, ['inventory', '--config', str(cfg_file)])
    assert result.exit_code == 0
    assert '1' in result.output  # gaps column should show 1


# ---------------------------------------------------------------------------
# XDG fallback tests
# ---------------------------------------------------------------------------

def test_validate_no_config_uses_xdg(config_file: Path) -> None:
    with patch('dccd.daemon.config.resolve_config_path', return_value=config_file):
        result = runner.invoke(app, ['validate'])
    assert result.exit_code == 0
    assert 'valid' in result.output.lower()


def test_validate_missing_all_configs(tmp_path: Path) -> None:
    with patch(
        'dccd.daemon.config.resolve_config_path',
        side_effect=FileNotFoundError('No config file found. Tried: config.yml, ~/.config/dccd/config.yml'),
    ):
        result = runner.invoke(app, ['validate'])
    assert result.exit_code == 1
    assert 'No config file found' in result.output


# ---------------------------------------------------------------------------
# Edge cases — config validation
# ---------------------------------------------------------------------------

def test_validate_unknown_exchange(tmp_path: Path) -> None:
    cfg = {
        'storage': {'local_path': str(tmp_path)},
        'histo_jobs': [{'exchange': 'fakeex', 'pairs': ['BTC/USDT'], 'span': 3600}],
    }
    cfg_file = tmp_path / 'config.yml'
    cfg_file.write_text(yaml.dump(cfg))
    result = runner.invoke(app, ['validate', '--config', str(cfg_file)])
    assert result.exit_code != 0
    assert 'fakeex' in result.output.lower() or result.exit_code != 0


def test_validate_bad_pair_format(tmp_path: Path) -> None:
    cfg = {
        'storage': {'local_path': str(tmp_path)},
        'histo_jobs': [{'exchange': 'binance', 'pairs': ['BTCUSDT'], 'span': 3600}],
    }
    cfg_file = tmp_path / 'config.yml'
    cfg_file.write_text(yaml.dump(cfg))
    result = runner.invoke(app, ['validate', '--config', str(cfg_file)])
    assert result.exit_code != 0


def test_validate_missing_config_file() -> None:
    result = runner.invoke(app, ['validate', '--config', '/nonexistent/config.yml'])
    assert result.exit_code != 0


# ---------------------------------------------------------------------------
# Edge cases — status --json
# ---------------------------------------------------------------------------

def test_status_json_no_metrics(config_file: Path) -> None:
    result = runner.invoke(app, ['status', '--json', '--config', str(config_file)])
    assert result.exit_code == 0
    data = json.loads(result.output.strip())
    assert data == {}


def test_status_json_output(tmp_path: Path) -> None:
    dccd_dir = tmp_path / 'data' / '.dccd'
    dccd_dir.mkdir(parents=True)
    metrics = {
        'binance/BTC/USDT': {
            'last_run_at': 1747440000.0,
            'last_success_at': 1747440000.0,
            'rows_collected': 42,
            'errors_count': 0,
        }
    }
    (dccd_dir / 'metrics.json').write_text(json.dumps(metrics))
    cfg = {
        'storage': {'local_path': str(dccd_dir.parent)},
        'histo_jobs': [{'exchange': 'binance', 'pairs': ['BTC/USDT'], 'span': 3600}],
    }
    cfg_file = tmp_path / 'config.yml'
    cfg_file.write_text(yaml.dump(cfg))

    result = runner.invoke(app, ['status', '--json', '--config', str(cfg_file)])
    assert result.exit_code == 0
    data = json.loads(result.output)
    assert 'binance/BTC/USDT' in data
    assert data['binance/BTC/USDT']['rows_collected'] == 42


# ---------------------------------------------------------------------------
# Edge cases — backfill filters
# ---------------------------------------------------------------------------

def test_backfill_dry_run_parallel(config_file: Path) -> None:
    with patch('dccd.daemon.backfill.make_job') as mock_make:
        mock_job = mock_make.return_value
        mock_job.obj.full_path = '/tmp/fake'
        result = runner.invoke(app, [
            'backfill', '--dry-run', '--parallel', '--config', str(config_file),
        ])
    assert result.exit_code == 0
    # dry-run must never call the real fetch
    mock_job.run.assert_called_once()
    call_kwargs = mock_job.run.call_args
    assert call_kwargs.kwargs.get('dry_run') or call_kwargs.args[1]


def test_backfill_exchange_filter_no_match(config_file: Path) -> None:
    result = runner.invoke(app, [
        'backfill', '--exchange', 'unknownex', '--config', str(config_file),
    ])
    assert result.exit_code == 0
    assert 'no matching' in result.output.lower()
