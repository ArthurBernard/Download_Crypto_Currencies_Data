#!/usr/bin/env python3
# coding: utf-8

from unittest.mock import MagicMock, patch

from apscheduler.schedulers.background import BackgroundScheduler

from dccd.daemon.config import CollectorConfig, HistoJob, StorageConfig
from dccd.daemon.scheduler import build_histo_scheduler, run_histo_job, run_once
from dccd.daemon.storage import RemoteStorage

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_config(histo_jobs=None, tmp_path=None):
    path = str(tmp_path) if tmp_path else '/data/crypto/'
    return CollectorConfig(
        storage=StorageConfig(local_path=path),
        histo_jobs=histo_jobs or [
            HistoJob(exchange='binance', pairs=['BTC/USDT', 'ETH/USDT'], span=3600),
            HistoJob(exchange='kraken', pairs=['BTC/USD'], span=86400),
        ],
    )


def _make_storage(tmp_path):
    return RemoteStorage(StorageConfig(local_path=str(tmp_path)))


# ---------------------------------------------------------------------------
# build_histo_scheduler
# ---------------------------------------------------------------------------

def test_scheduler_type(tmp_path):
    cfg = _make_config(tmp_path=tmp_path)
    scheduler = build_histo_scheduler(cfg)
    assert isinstance(scheduler, BackgroundScheduler)


def test_scheduler_job_count(tmp_path):
    cfg = _make_config(tmp_path=tmp_path)
    scheduler = build_histo_scheduler(cfg)
    jobs = scheduler.get_jobs()
    # 2 pairs (binance) + 1 pair (kraken) = 3 jobs
    assert len(jobs) == 3


def test_scheduler_job_ids(tmp_path):
    cfg = _make_config(tmp_path=tmp_path)
    scheduler = build_histo_scheduler(cfg)
    ids = {j.id for j in scheduler.get_jobs()}
    assert 'binance_BTC_USDT_3600' in ids
    assert 'binance_ETH_USDT_3600' in ids
    assert 'kraken_BTC_USD_86400' in ids


def test_scheduler_interval_seconds(tmp_path):
    cfg = _make_config(
        histo_jobs=[HistoJob(exchange='bybit', pairs=['BTC/USDT'], span=900)],
        tmp_path=tmp_path,
    )
    scheduler = build_histo_scheduler(cfg)
    job = scheduler.get_jobs()[0]
    assert job.trigger.interval.total_seconds() == 900


# ---------------------------------------------------------------------------
# run_histo_job
# ---------------------------------------------------------------------------

def _mock_exchange_cls(tmp_path_str):
    """Return a mock exchange class and instance pair."""
    mock_obj = MagicMock()
    mock_obj.import_data.return_value = mock_obj
    mock_obj.save.return_value = mock_obj
    mock_obj.full_path = tmp_path_str
    mock_cls = MagicMock(return_value=mock_obj)
    return mock_cls, mock_obj


def test_run_histo_job_calls_chain(tmp_path):
    import dccd.daemon.scheduler as sched_mod

    job = HistoJob(exchange='binance', pairs=['BTC/USDT'], span=3600)
    storage = MagicMock(spec=RemoteStorage)
    mock_cls, mock_obj = _mock_exchange_cls(str(tmp_path / 'Binance'))

    original = sched_mod._HISTO_CLASSES.copy()
    sched_mod._HISTO_CLASSES['binance'] = mock_cls
    try:
        run_histo_job(job, 'BTC/USDT', str(tmp_path), storage)
    finally:
        sched_mod._HISTO_CLASSES.update(original)

    mock_cls.assert_called_once_with(str(tmp_path), 'BTC', 3600, 'USDT', form='parquet')
    mock_obj.import_data.assert_called_once_with('last', 'now')
    mock_obj.save.assert_called_once_with(form='parquet', by_period='Y')
    storage.push.assert_called_once_with(str(tmp_path / 'Binance'))


def test_run_histo_job_splits_pair_correctly(tmp_path):
    import dccd.daemon.scheduler as sched_mod

    job = HistoJob(exchange='okx', pairs=['ETH/USDT'], span=3600)
    storage = MagicMock(spec=RemoteStorage)
    mock_cls, mock_obj = _mock_exchange_cls('/tmp')
    mock_obj.full_path = '/tmp'

    original = sched_mod._HISTO_CLASSES.copy()
    sched_mod._HISTO_CLASSES['okx'] = mock_cls
    try:
        run_histo_job(job, 'ETH/USDT', str(tmp_path), storage)
    finally:
        sched_mod._HISTO_CLASSES.update(original)

    mock_cls.assert_called_once_with(str(tmp_path), 'ETH', 3600, 'USDT', form='parquet')


# ---------------------------------------------------------------------------
# run_once
# ---------------------------------------------------------------------------

def test_run_once_executes_all_jobs(tmp_path):
    cfg = _make_config(tmp_path=tmp_path)

    with patch('dccd.daemon.scheduler.run_histo_job') as mock_job:
        run_once(cfg)

    assert mock_job.call_count == 3  # BTC/USDT, ETH/USDT (binance) + BTC/USD (kraken)


def test_run_once_job_failure_does_not_stop_others(tmp_path):
    cfg = _make_config(tmp_path=tmp_path)
    call_log = []

    def _side_effect(job, pair, *args, **kwargs):
        call_log.append(pair)
        if pair == 'BTC/USDT' and job.exchange == 'binance':
            raise RuntimeError('network error')

    with patch('dccd.daemon.scheduler.run_histo_job', side_effect=_side_effect):
        run_once(cfg)  # must not raise

    assert 'ETH/USDT' in call_log
    assert 'BTC/USD' in call_log


def test_run_once_logs_exception_on_failure(tmp_path, caplog):
    cfg = _make_config(
        histo_jobs=[HistoJob(exchange='binance', pairs=['BTC/USDT'], span=3600)],
        tmp_path=tmp_path,
    )

    import logging
    with patch('dccd.daemon.scheduler.run_histo_job', side_effect=ValueError('boom')):
        with caplog.at_level(logging.ERROR, logger='dccd.daemon.scheduler'):
            run_once(cfg)

    assert 'histo job failed' in caplog.text
