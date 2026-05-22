#!/usr/bin/env python3
# coding: utf-8

"""Tests for dccd.daemon.backfill."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from dccd.daemon.backfill import (
    _EXCHANGE_DEFAULTS,
    KrakenBackfill,
    OHLCBackfill,
    make_job,
    run_backfill,
)
from dccd.daemon.cli import app

# ---------------------------------------------------------------------------
# _trades_to_ohlc
# ---------------------------------------------------------------------------

def _make_kraken_job(tmp_path) -> KrakenBackfill:
    """Return a KrakenBackfill with a mocked exchange object."""
    obj = MagicMock()
    obj.span = 60
    obj.tz = 'UTC'
    obj.crypto = 'BTC'
    obj.fiat = 'USD'
    obj.pair = 'XBTUSD'
    obj.df = None
    return KrakenBackfill(obj, sleep=0.0, form='parquet')


def test_trades_to_ohlc_basic():
    job = _make_kraken_job('/tmp')
    start = 1700000000
    end   = start + 300  # 5 minutes

    trades = [
        {'timestamp': start + 10,  'price': 100.0, 'amount': 1.0},
        {'timestamp': start + 20,  'price': 101.0, 'amount': 2.0},
        {'timestamp': start + 70,  'price': 102.0, 'amount': 0.5},
        {'timestamp': start + 200, 'price':  99.0, 'amount': 3.0},
    ]
    candles = job._trades_to_ohlc(trades, start, end)

    assert len(candles) == 3  # minutes 0, 1, 3 have trades; minute 2 and 4 empty → dropped
    assert candles[0]['open'] == 100.0
    assert candles[0]['high'] == 101.0
    assert candles[0]['low']  == 100.0
    assert candles[0]['close'] == 101.0
    assert candles[0]['volume'] == pytest.approx(3.0)
    assert candles[1]['open'] == 102.0


def test_trades_to_ohlc_empty():
    job = _make_kraken_job('/tmp')
    assert job._trades_to_ohlc([], 1700000000, 1700000300) == []


def test_trades_to_ohlc_weighted_average():
    job = _make_kraken_job('/tmp')
    start = 1700000000
    trades = [
        {'timestamp': start + 5,  'price': 100.0, 'amount': 1.0},
        {'timestamp': start + 10, 'price': 200.0, 'amount': 1.0},
    ]
    candles = job._trades_to_ohlc(trades, start, start + 60)
    assert len(candles) == 1
    assert candles[0]['weightedAverage'] == pytest.approx(150.0)


# ---------------------------------------------------------------------------
# make_job
# ---------------------------------------------------------------------------

def test_make_job_binance_returns_ohlc(tmp_path):
    mock_obj = MagicMock()
    mock_obj.span = 60
    mock_obj.crypto = 'BTC'
    mock_obj.fiat = 'USDT'
    with patch('dccd.daemon.scheduler._HISTO_CLASSES',
               {'binance': lambda *a, **kw: mock_obj}):
        job = make_job('binance', 'BTC', 'USDT', 60, str(tmp_path), 'UTC', 'parquet')
    assert isinstance(job, OHLCBackfill)
    assert job.max_candles == _EXCHANGE_DEFAULTS['binance']['max_candles']


def test_make_job_kraken_returns_kraken_backfill(tmp_path):
    mock_obj = MagicMock()
    mock_obj.span = 60
    mock_obj.crypto = 'BTC'
    mock_obj.fiat = 'USD'
    with patch('dccd.daemon.scheduler._HISTO_CLASSES',
               {'kraken': lambda *a, **kw: mock_obj}):
        job = make_job('kraken', 'BTC', 'USD', 60, str(tmp_path), 'UTC', 'parquet')
    assert isinstance(job, KrakenBackfill)


def test_make_job_unsupported_exchange(tmp_path):
    with pytest.raises(ValueError, match='Unsupported exchange'):
        make_job('fakex', 'BTC', 'USD', 60, str(tmp_path), 'UTC', 'parquet')


# ---------------------------------------------------------------------------
# dry_run (no network calls)
# ---------------------------------------------------------------------------

def _mock_obj(span=60):
    obj = MagicMock()
    obj.span = span
    obj.tz = 'UTC'
    obj.crypto = 'BTC'
    obj.fiat = 'USDT'
    obj.df = None
    return obj


def test_ohlc_backfill_dry_run_no_network():
    obj = _mock_obj()
    job = OHLCBackfill(obj, max_candles=990, sleep=0.0, form='parquet')
    job.run('2026-01-01 00:00:00', dry_run=True)
    obj._get_last_date.assert_not_called()
    obj.import_data.assert_not_called()
    obj.save.assert_not_called()


def test_kraken_backfill_dry_run_no_network():
    obj = _mock_obj()
    job = KrakenBackfill(obj, sleep=0.0, form='parquet')
    job.run('2026-01-01 00:00:00', dry_run=True)
    obj._get_last_date.assert_not_called()
    obj._fetch.assert_not_called()
    obj.save.assert_not_called()


# ---------------------------------------------------------------------------
# run_backfill — exchange / pair filtering
# ---------------------------------------------------------------------------

def _make_cfg(exchanges=('binance', 'kraken')):
    from dccd.daemon.config import CollectorConfig, HistoJob, SettingsConfig

    jobs = [
        HistoJob(exchange=ex, pairs=['BTC/USDT' if ex != 'kraken' else 'BTC/USD'], span=60)
        for ex in exchanges
    ]
    return CollectorConfig(
        settings=SettingsConfig(data_path='/tmp/dccd_test', timezone='UTC'),
        histo_jobs=jobs,
    )


def test_run_backfill_exchange_filter(tmp_path):
    cfg = _make_cfg()
    called = []

    def fake_run(start, dry_run=False, position=0):
        called.append(self_label)

    with patch('dccd.daemon.backfill.make_job') as mock_make:
        mock_job = MagicMock()
        mock_job.label = 'binance  BTC/USDT'

        def capture_label(*args, **kwargs):
            nonlocal self_label
            self_label = args[0] + '/' + args[1]
            mock_job.run = fake_run
            return mock_job

        mock_make.side_effect = capture_label
        self_label = ''
        run_backfill(cfg, exchange='binance', dry_run=True)

    exchanges_called = [c.split('/')[0] for c in called]
    assert all(e == 'binance' for e in exchanges_called)
    assert 'kraken' not in exchanges_called


# ---------------------------------------------------------------------------
# CLI — backfill command via typer CliRunner
# ---------------------------------------------------------------------------

_RUNNER = CliRunner()


def test_cli_backfill_dry_run(tmp_path):
    config_file = tmp_path / 'config.yml'
    config_file.write_text(
        'settings:\n'
        f'  data_path: {tmp_path}\n'
        '  timezone: UTC\n'
        'histo_jobs:\n'
        '  - exchange: binance\n'
        '    pairs: [BTC/USDT]\n'
        '    span: 60\n'
    )
    with patch('dccd.daemon.backfill.make_job') as mock_make:
        mock_job = MagicMock()
        mock_job.label = 'Binance  BTC/USDT'
        mock_make.return_value = mock_job
        result = _RUNNER.invoke(
            app,
            ['backfill', '--config', str(config_file), '--dry-run'],
        )

    assert result.exit_code == 0
    mock_job.run.assert_called_once_with(
        '2020-01-01 00:00:00', dry_run=True, position=0,
    )


def test_cli_backfill_missing_config(tmp_path):
    result = _RUNNER.invoke(
        app,
        ['backfill', '--config', str(tmp_path / 'missing.yml')],
    )
    assert result.exit_code == 1
