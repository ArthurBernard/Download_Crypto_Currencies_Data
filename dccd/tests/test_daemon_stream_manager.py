#!/usr/bin/env python3
# coding: utf-8

import time
from unittest.mock import MagicMock, patch

import pytest

from dccd.daemon.config import CollectorConfig, StorageConfig, StreamJob
from dccd.daemon.stream_manager import (
    StreamManager,
    SyncService,
    _connect_kwargs,
    _format_pair,
    _iter_tasks,
    _process_fn,
)
from dccd.process_data import set_marketdepth, set_orders, set_trades

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _storage_cfg(tmp_path, remotes=None, sync_interval=3600):
    remotes = remotes or []
    return StorageConfig(
        local_path=str(tmp_path),
        remotes=remotes,
        sync_interval=sync_interval,
    )


def _stream_job(exchange='binance', pairs=None, channels=None, time_step=60):
    return StreamJob(
        exchange=exchange,
        pairs=pairs or ['BTC/USDT'],
        channels=channels or ['trades', 'book'],
        time_step=time_step,
    )


def _make_config(tmp_path, jobs=None, remotes=None):
    return CollectorConfig(
        storage=_storage_cfg(tmp_path, remotes=remotes),
        stream_jobs=jobs or [_stream_job()],
    )


# ---------------------------------------------------------------------------
# _format_pair
# ---------------------------------------------------------------------------

def test_format_pair_binance():
    assert _format_pair('binance', 'BTC/USDT') == 'BTCUSDT'


def test_format_pair_bybit():
    assert _format_pair('bybit', 'ETH/USDT') == 'ETHUSDT'


def test_format_pair_kraken():
    assert _format_pair('kraken', 'BTC/USD') == 'BTC/USD'


def test_format_pair_okx():
    assert _format_pair('okx', 'BTC/USDT') == 'BTC-USDT'


def test_format_pair_bitfinex_usd():
    assert _format_pair('bitfinex', 'BTC/USD') == 'tBTCUSD'


def test_format_pair_bitfinex_usdt():
    assert _format_pair('bitfinex', 'BTC/USDT') == 'tBTCUST'


def test_format_pair_bitmex_btc():
    assert _format_pair('bitmex', 'BTC/USD') == 'XBTUSD'


def test_format_pair_bitmex_eth():
    assert _format_pair('bitmex', 'ETH/USD') == 'ETHUSD'


def test_format_pair_unsupported_raises():
    with pytest.raises(ValueError, match='not supported'):
        _format_pair('poloniex', 'BTC/USDT')


# ---------------------------------------------------------------------------
# _process_fn
# ---------------------------------------------------------------------------

def test_process_fn_trades():
    assert _process_fn(['trades']) is set_trades


def test_process_fn_book():
    assert _process_fn(['book']) is set_marketdepth


def test_process_fn_both():
    assert _process_fn(['trades', 'book']) is set_orders


def test_process_fn_fallback():
    assert _process_fn(['kline']) is set_orders


# ---------------------------------------------------------------------------
# _connect_kwargs
# ---------------------------------------------------------------------------

def test_connect_kwargs_binance():
    assert _connect_kwargs('binance', 'BTC/USDT', ['trades']) == {}


def test_connect_kwargs_kraken():
    assert _connect_kwargs('kraken', 'BTC/USD', ['trades', 'book']) == {}


def test_connect_kwargs_bitfinex_trades():
    kw = _connect_kwargs('bitfinex', 'BTC/USD', ['trades'])
    assert kw == {'channel': 'trades', 'symbol': 'tBTCUSD'}


def test_connect_kwargs_bitmex_book():
    kw = _connect_kwargs('bitmex', 'BTC/USD', ['book'])
    assert kw == {'args': 'orderBookL2_25:XBTUSD'}


# ---------------------------------------------------------------------------
# _iter_tasks
# ---------------------------------------------------------------------------

def test_iter_tasks_binance_single_thread_for_all_channels():
    job = _stream_job(exchange='binance', pairs=['BTC/USDT'], channels=['trades', 'book'])
    tasks = list(_iter_tasks(job))
    assert len(tasks) == 1
    pair, channels = tasks[0]
    assert pair == 'BTC/USDT'
    assert 'trades' in channels and 'book' in channels


def test_iter_tasks_bitfinex_one_thread_per_channel():
    job = _stream_job(exchange='bitfinex', pairs=['BTC/USD'], channels=['trades', 'book'])
    tasks = list(_iter_tasks(job))
    assert len(tasks) == 2
    channel_lists = [ch for _, ch in tasks]
    assert ['trades'] in channel_lists
    assert ['book'] in channel_lists


def test_iter_tasks_bitmex_one_thread_per_channel():
    job = _stream_job(exchange='bitmex', pairs=['BTC/USD'], channels=['trades', 'book'])
    tasks = list(_iter_tasks(job))
    assert len(tasks) == 2


def test_iter_tasks_multiple_pairs():
    job = _stream_job(exchange='binance', pairs=['BTC/USDT', 'ETH/USDT'], channels=['trades'])
    tasks = list(_iter_tasks(job))
    assert len(tasks) == 2


# ---------------------------------------------------------------------------
# SyncService
# ---------------------------------------------------------------------------

def test_sync_service_no_remotes_no_thread(tmp_path):
    svc = SyncService(_storage_cfg(tmp_path, remotes=[], sync_interval=60))
    svc.start()
    assert svc._thread is None


def test_sync_service_sync_interval_zero_no_thread(tmp_path):
    from dccd.daemon.config import RemoteConfig
    svc = SyncService(_storage_cfg(
        tmp_path,
        remotes=[RemoteConfig(provider='rclone', remote='mynas:crypto/')],
        sync_interval=0,
    ))
    svc.start()
    assert svc._thread is None


def test_sync_service_stop_sets_event(tmp_path):
    svc = SyncService(_storage_cfg(tmp_path))
    svc.stop()
    assert svc._stop.is_set()


def test_sync_service_sync_now_calls_push(tmp_path):
    from dccd.daemon.config import RemoteConfig
    svc = SyncService(_storage_cfg(
        tmp_path,
        remotes=[RemoteConfig(provider='rclone', remote='mynas:crypto/')],
    ))
    with patch.object(svc._storage, 'push') as mock_push:
        svc.sync_now()
    mock_push.assert_called_once_with(str(tmp_path))


# ---------------------------------------------------------------------------
# StreamManager.start / stop
# ---------------------------------------------------------------------------

def test_stream_manager_start_creates_threads(tmp_path):
    cfg = _make_config(tmp_path, jobs=[
        _stream_job(exchange='binance', pairs=['BTC/USDT', 'ETH/USDT']),
    ])
    mgr = StreamManager(cfg)

    with patch.object(mgr, '_run_forever'):  # don't actually run
        with patch.object(mgr._sync, 'start'):
            mgr.start()

    assert len(mgr._threads) == 2


def test_stream_manager_start_starts_sync(tmp_path):
    cfg = _make_config(tmp_path)
    mgr = StreamManager(cfg)

    with patch.object(mgr, '_run_forever'):
        with patch.object(mgr._sync, 'start') as mock_sync_start:
            mgr.start()

    mock_sync_start.assert_called_once()


def test_stream_manager_bitfinex_two_channels_two_threads(tmp_path):
    cfg = _make_config(tmp_path, jobs=[
        _stream_job(exchange='bitfinex', pairs=['BTC/USD'], channels=['trades', 'book']),
    ])
    mgr = StreamManager(cfg)

    with patch.object(mgr, '_run_forever'):
        with patch.object(mgr._sync, 'start'):
            mgr.start()

    assert len(mgr._threads) == 2


def test_stream_manager_stop_sets_event(tmp_path):
    cfg = _make_config(tmp_path)
    mgr = StreamManager(cfg)
    with patch.object(mgr._sync, 'stop'):
        mgr.stop()
    assert mgr._stop_event.is_set()


def test_stream_manager_stop_signals_downloaders(tmp_path):
    cfg = _make_config(tmp_path)
    mgr = StreamManager(cfg)

    dl = MagicMock()
    dl.until = time.time() + 9999
    dl.is_connect = True
    mgr._downloaders['fake'] = dl

    with patch.object(mgr._sync, 'stop'):
        mgr.stop()

    assert dl.is_connect is False
    assert dl.until <= time.time()


# ---------------------------------------------------------------------------
# StreamManager._run_forever
# ---------------------------------------------------------------------------

def test_run_forever_stops_immediately_if_stop_set(tmp_path):
    cfg = _make_config(tmp_path)
    mgr = StreamManager(cfg)
    mgr._stop_event.set()

    with patch.object(mgr, '_run_once') as mock_once:
        mgr._run_forever(_stream_job(), 'BTC/USDT', ['trades'])

    mock_once.assert_not_called()


def test_run_forever_restarts_on_exception(tmp_path):
    cfg = _make_config(tmp_path)
    mgr = StreamManager(cfg)
    call_count = 0

    def _side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError('crash')
        mgr._stop_event.set()  # stop after second call

    with patch.object(mgr, '_run_once', side_effect=_side_effect):
        with patch.object(mgr._stop_event, 'wait', return_value=False):
            mgr._run_forever(_stream_job(), 'BTC/USDT', ['trades'])

    assert call_count == 2


# ---------------------------------------------------------------------------
# StreamManager._run_once (unit, mocked downloader)
# ---------------------------------------------------------------------------

def _patch_stream_classes(exchange, mock_cls):
    """Context manager that replaces one entry in _STREAM_CLASSES."""
    import dccd.daemon.stream_manager as sm
    original = sm._STREAM_CLASSES.copy()
    sm._STREAM_CLASSES[exchange] = mock_cls
    return original


def test_run_once_configures_downloader(tmp_path):
    import dccd.daemon.stream_manager as sm

    job = _stream_job(exchange='binance', channels=['trades'])
    cfg = _make_config(tmp_path, jobs=[job])
    mgr = StreamManager(cfg)

    mock_obj = MagicMock()
    mock_cls = MagicMock(return_value=mock_obj)

    original = sm._STREAM_CLASSES.copy()
    sm._STREAM_CLASSES['binance'] = mock_cls
    try:
        with patch('asyncio.new_event_loop') as mock_loop_fn, \
             patch('asyncio.set_event_loop'), \
             patch('asyncio.gather'):
            mock_loop = MagicMock()
            mock_loop_fn.return_value = mock_loop
            mock_loop.run_until_complete.side_effect = None
            mgr._run_once(job, 'BTC/USDT', ['trades'])
    finally:
        sm._STREAM_CLASSES.update(original)

    mock_cls.assert_called_once_with(pair='BTCUSDT', time_step=60, until=0)
    mock_obj.set_process_data.assert_called_once_with(set_trades)
    mock_obj.set_saver.assert_called_once()


def test_run_once_bitfinex_sets_parser(tmp_path):
    import dccd.daemon.stream_manager as sm

    job = _stream_job(exchange='bitfinex', channels=['trades'])
    cfg = _make_config(tmp_path, jobs=[job])
    mgr = StreamManager(cfg)

    mock_obj = MagicMock()
    mock_obj.get_parser.return_value = MagicMock()
    mock_cls = MagicMock(return_value=mock_obj)

    original = sm._STREAM_CLASSES.copy()
    sm._STREAM_CLASSES['bitfinex'] = mock_cls
    try:
        with patch('asyncio.new_event_loop') as mock_loop_fn, \
             patch('asyncio.set_event_loop'), \
             patch('asyncio.gather'):
            mock_loop = MagicMock()
            mock_loop_fn.return_value = mock_loop
            mock_loop.run_until_complete.side_effect = None
            mgr._run_once(job, 'BTC/USD', ['trades'])
    finally:
        sm._STREAM_CLASSES.update(original)

    mock_obj.get_parser.assert_called_once_with('trades')
    assert mock_obj.parser is mock_obj.get_parser.return_value
