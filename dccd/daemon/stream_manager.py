#!/usr/bin/env python3
# coding: utf-8

""" Real-time stream manager and periodic sync service for the dccd daemon.

:class:`SyncService` runs a background thread that pushes the entire local
data directory to all configured remotes at a fixed interval.

:class:`StreamManager` starts one background thread per ``(exchange, pair)``
combination (or per ``(exchange, pair, channel)`` for Bitfinex/Bitmex) and
restarts them automatically on failure.

"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from dccd.continuous_dl.binance import DownloadBinanceData
from dccd.continuous_dl.bitfinex import DownloadBitfinexData
from dccd.continuous_dl.bitmex import DownloadBitmexData
from dccd.continuous_dl.bybit import DownloadBybitData
from dccd.continuous_dl.exchange import ContinuousDownloader
from dccd.continuous_dl.kraken import DownloadKrakenData
from dccd.continuous_dl.okx import DownloadOKXData
from dccd.daemon.storage import RemoteStorage
from dccd.process_data import set_marketdepth, set_orders, set_trades
from dccd.tools.io import IODataBase

if TYPE_CHECKING:
    from dccd.daemon.config import CollectorConfig, StorageConfig, StreamJob

__all__ = ['StreamManager', 'SyncService']

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Exchange class registry
# ---------------------------------------------------------------------------

_STREAM_CLASSES: dict[str, Any] = {
    'binance':  DownloadBinanceData,
    'bybit':    DownloadBybitData,
    'kraken':   DownloadKrakenData,
    'okx':      DownloadOKXData,
    'bitfinex': DownloadBitfinexData,
    'bitmex':   DownloadBitmexData,
}

# Bitfinex/Bitmex use one WS connection per channel → one thread per channel.
# Other exchanges bundle all channels in one connection → one thread per pair.
_PER_CHANNEL_EXCHANGES = frozenset({'bitfinex', 'bitmex'})

# Channel name mappings for legacy exchanges
_BITFINEX_CHANNEL: dict[str, str] = {'trades': 'trades', 'book': 'book'}
_BITMEX_CHANNEL:   dict[str, str] = {'trades': 'trade',  'book': 'orderBookL2_25'}

_RESTART_DELAY = 30  # seconds between stream restarts after a crash


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _format_pair(exchange: str, pair: str) -> str:
    """ Convert ``'BTC/USDT'`` to the exchange-specific pair format.

    Parameters
    ----------
    exchange : str
        Exchange name (lowercase).
    pair : str
        Trading pair in ``'CRYPTO/FIAT'`` format.

    Returns
    -------
    str
        Exchange-specific pair string.

    Raises
    ------
    ValueError
        If *exchange* is not supported for streaming.

    """
    crypto, fiat = pair.split('/', 1)
    if exchange in ('binance', 'bybit'):
        return crypto + fiat
    if exchange == 'kraken':
        return pair
    if exchange == 'okx':
        return f'{crypto}-{fiat}'
    if exchange == 'bitfinex':
        fiat_bf = 'UST' if fiat == 'USDT' else fiat
        return f't{crypto}{fiat_bf}'
    if exchange == 'bitmex':
        xbt = 'XBT' if crypto == 'BTC' else crypto
        return xbt + fiat
    raise ValueError(f'exchange {exchange!r} is not supported for streaming')


def _process_fn(channels: list[str]) -> Any:
    """ Return the appropriate ``process_data`` function for *channels*.

    Parameters
    ----------
    channels : list of str
        Channel names (``'trades'``, ``'book'``, …).

    Returns
    -------
    callable
        One of :func:`~dccd.process_data.set_trades`,
        :func:`~dccd.process_data.set_marketdepth`,
        :func:`~dccd.process_data.set_orders`.

    """
    has_trades = 'trades' in channels
    has_book   = 'book'   in channels
    if has_trades and has_book:
        return set_orders
    if has_trades:
        return set_trades
    if has_book:
        return set_marketdepth
    return set_orders


def _connect_kwargs(exchange: str, pair: str, channels: list[str]) -> dict[str, Any]:
    """ Return keyword arguments to pass to ``downloader._connect()``.

    Parameters
    ----------
    exchange : str
        Exchange name.
    pair : str
        Trading pair in ``'CRYPTO/FIAT'`` format.
    channels : list of str
        Channel(s) for this thread (singleton for Bitfinex/Bitmex).

    Returns
    -------
    dict
        Empty for exchanges that embed subscription in ``__init__``;
        channel/symbol kwargs for Bitfinex; ``args`` string for Bitmex.

    """
    ch = channels[0]
    if exchange == 'bitfinex':
        return {'channel': _BITFINEX_CHANNEL.get(ch, ch),
                'symbol':  _format_pair('bitfinex', pair)}
    if exchange == 'bitmex':
        bch = _BITMEX_CHANNEL.get(ch, ch)
        return {'args': f'{bch}:{_format_pair("bitmex", pair)}'}
    return {}


def _iter_tasks(job: StreamJob) -> Iterator[tuple[str, list[str]]]:
    """ Yield ``(pair, channels)`` for each thread to create.

    Bitfinex and Bitmex have one WS connection per channel, so each
    ``(pair, channel)`` pair gets its own thread.  All other exchanges
    handle multiple channels in one connection.

    Parameters
    ----------
    job : StreamJob
        Stream job configuration.

    Yields
    ------
    pair : str
    channels : list of str

    """
    if job.exchange in _PER_CHANNEL_EXCHANGES:
        for pair in job.pairs:
            for ch in job.channels:
                yield pair, [ch]
    else:
        for pair in job.pairs:
            yield pair, job.channels


# ---------------------------------------------------------------------------
# SyncService
# ---------------------------------------------------------------------------

class SyncService:
    """ Periodically push the entire local data directory to all remotes.

    This is the single point of truth for remote synchronisation.  Neither
    histo jobs nor stream threads push data themselves — they save locally
    and rely on this service to replicate to remote destinations.

    Parameters
    ----------
    config : StorageConfig
        Storage configuration (``remotes`` list + ``sync_interval``).

    Notes
    -----
    If ``config.remotes`` is empty or ``config.sync_interval`` is 0, the
    service is a no-op and no background thread is started.

    """

    def __init__(self, config: StorageConfig) -> None:
        self.config = config
        self._storage = RemoteStorage(config)
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """ Start the background sync thread (idempotent). """
        if not self.config.remotes or self.config.sync_interval <= 0:
            logger.info(
                'SyncService disabled (remotes=%d, sync_interval=%d)',
                len(self.config.remotes), self.config.sync_interval,
            )
            return
        self._thread = threading.Thread(
            target=self._loop, daemon=True, name='sync-service',
        )
        self._thread.start()
        logger.info('SyncService started (interval=%ds)', self.config.sync_interval)

    def stop(self) -> None:
        """ Signal the sync thread to stop at the next interval boundary. """
        self._stop.set()

    def sync_now(self) -> None:
        """ Push ``local_path`` to all remotes immediately (blocking). """
        self._storage.push(self.config.local_path)

    def _loop(self) -> None:
        while not self._stop.wait(timeout=self.config.sync_interval):
            try:
                self.sync_now()
            except Exception:
                logger.exception('SyncService: sync failed')


# ---------------------------------------------------------------------------
# StreamManager
# ---------------------------------------------------------------------------

class StreamManager:
    """ Manage real-time WebSocket collection jobs.

    Starts one background thread per ``(exchange, pair)`` (or per
    ``(exchange, pair, channel)`` for Bitfinex/Bitmex).  Each thread
    runs indefinitely and is automatically restarted after a crash.
    A :class:`SyncService` instance pushes data to remotes periodically.

    Parameters
    ----------
    config : CollectorConfig
        Daemon configuration (``stream_jobs`` + ``storage``).

    """

    def __init__(self, config: CollectorConfig) -> None:
        self.config = config
        self._threads:     dict[str, threading.Thread]     = {}
        self._downloaders: dict[str, ContinuousDownloader] = {}
        self._stop_event = threading.Event()
        self._sync = SyncService(config.storage)

    def start(self) -> None:
        """ Start the sync service and all stream threads. """
        self._sync.start()
        for job in self.config.stream_jobs:
            for pair, channels in _iter_tasks(job):
                ch_tag = '_'.join(channels)
                key = f'{job.exchange}_{pair.replace("/", "_")}_{ch_tag}'
                t = threading.Thread(
                    target=self._run_forever,
                    args=(job, pair, channels),
                    name=key,
                    daemon=True,
                )
                self._threads[key] = t
                t.start()
                logger.info('stream started: %s %s channels=%s', job.exchange, pair, channels)

    def stop(self) -> None:
        """ Signal all streams and the sync service to stop. """
        self._stop_event.set()
        self._sync.stop()
        for dl in self._downloaders.values():
            dl.until = time.time()
            dl.is_connect = False

    # ------------------------------------------------------------------
    # Thread body
    # ------------------------------------------------------------------

    def _run_forever(self, job: StreamJob, pair: str, channels: list[str]) -> None:
        while not self._stop_event.is_set():
            try:
                self._run_once(job, pair, channels)
            except Exception:
                logger.exception('stream crashed: %s %s', job.exchange, pair)
            if not self._stop_event.is_set():
                self._stop_event.wait(timeout=_RESTART_DELAY)

    def _run_once(self, job: StreamJob, pair: str, channels: list[str]) -> None:
        cls = _STREAM_CLASSES[job.exchange]

        # Bitfinex/Bitmex do not take pair in __init__; they receive it
        # via _connect() kwargs.  All other exchanges set the pair in __init__.
        if job.exchange in _PER_CHANNEL_EXCHANGES:
            downloader: ContinuousDownloader = cls(
                time_step=job.time_step, until=0,
            )
            # self.parser must be initialised before _loop() / on_message()
            ch_map = _BITFINEX_CHANNEL if job.exchange == 'bitfinex' else _BITMEX_CHANNEL
            ch_key = ch_map.get(channels[0], channels[0])
            downloader.parser = downloader.get_parser(ch_key)  # type: ignore[attr-defined]
        else:
            downloader = cls(
                pair=_format_pair(job.exchange, pair),
                time_step=job.time_step,
                until=0,
            )

        ch_tag = '_'.join(channels)
        key = f'{job.exchange}_{pair.replace("/", "_")}_{ch_tag}'
        self._downloaders[key] = downloader

        xch = job.exchange.capitalize()
        save_path = (
            f'{self.config.storage.local_path.rstrip("/")}'
            f'/{xch}/Data/WS_Data/{job.time_step}s/{pair.replace("/", "_")}'
        )

        downloader.set_process_data(_process_fn(channels))
        downloader.set_saver(IODataBase(save_path, method='csv'))

        conn_kw = _connect_kwargs(job.exchange, pair, channels)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(asyncio.gather(
                downloader._connect(**conn_kw),
                downloader._loop(),
            ))
        finally:
            loop.close()
            self._downloaders.pop(key, None)
            logger.info('stream ended: %s %s', job.exchange, pair)
