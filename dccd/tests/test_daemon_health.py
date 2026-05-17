#!/usr/bin/env python3
# coding: utf-8

"""Tests for dccd.daemon.health."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from dccd.daemon.config import AlertConfig
from dccd.daemon.health import HealthMonitor


@pytest.fixture()
def alert_cfg() -> AlertConfig:
    return AlertConfig(webhook_url=None, max_consecutive_errors=3)


@pytest.fixture()
def monitor(tmp_path: Path, alert_cfg: AlertConfig) -> HealthMonitor:
    return HealthMonitor(tmp_path, alert_cfg)


def test_record_success_updates_metrics(monitor: HealthMonitor, tmp_path: Path) -> None:
    monitor.record_success('binance', 'BTC/USDT', rows=10)
    m = monitor.get_metrics()['binance/BTC/USDT']
    assert m.last_run_at is not None
    assert m.last_success_at == m.last_run_at
    assert m.rows_collected == 10
    assert m.errors_count == 0


def test_record_success_resets_errors(monitor: HealthMonitor) -> None:
    monitor.record_failure('binance', 'BTC/USDT')
    monitor.record_failure('binance', 'BTC/USDT')
    monitor.record_success('binance', 'BTC/USDT')
    assert monitor.get_metrics()['binance/BTC/USDT'].errors_count == 0


def test_record_failure_increments(monitor: HealthMonitor) -> None:
    monitor.record_failure('kraken', 'ETH/USD')
    monitor.record_failure('kraken', 'ETH/USD')
    m = monitor.get_metrics()['kraken/ETH/USD']
    assert m.errors_count == 2
    assert m.last_run_at is not None
    assert m.last_success_at is None


def test_record_failure_triggers_webhook(tmp_path: Path) -> None:
    cfg = AlertConfig(webhook_url='http://example.com/hook', max_consecutive_errors=2)
    mon = HealthMonitor(tmp_path, cfg)

    with patch('urllib.request.urlopen') as mock_open:
        mon.record_failure('binance', 'BTC/USDT')
        mock_open.assert_not_called()
        mon.record_failure('binance', 'BTC/USDT')
        mock_open.assert_called_once()


def test_no_alert_below_threshold(tmp_path: Path) -> None:
    cfg = AlertConfig(webhook_url='http://example.com/hook', max_consecutive_errors=5)
    mon = HealthMonitor(tmp_path, cfg)
    with patch('urllib.request.urlopen') as mock_open:
        for _ in range(4):
            mon.record_failure('binance', 'BTC/USDT')
        mock_open.assert_not_called()


def test_metrics_persist_reload(tmp_path: Path, alert_cfg: AlertConfig) -> None:
    mon1 = HealthMonitor(tmp_path, alert_cfg)
    mon1.record_success('binance', 'BTC/USDT', rows=42)
    mon1.record_failure('kraken', 'ETH/USD')

    mon2 = HealthMonitor(tmp_path, alert_cfg)
    metrics = mon2.get_metrics()
    assert metrics['binance/BTC/USDT'].rows_collected == 42
    assert metrics['kraken/ETH/USD'].errors_count == 1


def test_rotating_log_created(monitor: HealthMonitor, tmp_path: Path) -> None:
    log_file = tmp_path / '.dccd' / 'dccd.log'
    assert log_file.exists()
