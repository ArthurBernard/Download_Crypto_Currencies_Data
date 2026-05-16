#!/usr/bin/env python3
# coding: utf-8

""" Historical data scheduler for the dccd daemon.

Wraps APScheduler 3.x BackgroundScheduler to run periodic REST API
collection jobs defined in a :class:`~dccd.daemon.config.CollectorConfig`.

"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from apscheduler.schedulers.background import BackgroundScheduler

from dccd.histo_dl.binance import FromBinance
from dccd.histo_dl.bybit import FromBybit
from dccd.histo_dl.coinbase import FromCoinbase
from dccd.histo_dl.exchange import ImportDataCryptoCurrencies
from dccd.histo_dl.kraken import FromKraken
from dccd.histo_dl.okx import FromOKX

if TYPE_CHECKING:
    from dccd.daemon.config import CollectorConfig, HistoJob

__all__ = ['build_histo_scheduler', 'run_histo_job', 'run_once']

logger = logging.getLogger(__name__)

_HISTO_CLASSES: dict[str, type[ImportDataCryptoCurrencies]] = {
    'binance': FromBinance,
    'kraken': FromKraken,
    'bybit': FromBybit,
    'okx': FromOKX,
    'coinbase': FromCoinbase,
}


def run_histo_job(job: HistoJob, pair: str, base_path: str) -> None:
    """ Download and save one (exchange, pair) candle job locally.

    Data is saved to ``base_path`` on the daemon host.  Remote sync is
    handled separately by :class:`~dccd.daemon.stream_manager.SyncService`.

    Parameters
    ----------
    job : HistoJob
        Job configuration (exchange, span, format, by_period).
    pair : str
        Trading pair in ``'CRYPTO/FIAT'`` format (e.g. ``'BTC/USDT'``).
    base_path : str
        Root directory for local storage (``CollectorConfig.storage.local_path``).

    """
    crypto, fiat = pair.split('/', 1)
    cls = _HISTO_CLASSES[job.exchange]
    obj = cls(base_path, crypto, job.span, fiat, form=job.format)
    obj.import_data('last', 'now').save(form=job.format, by_period=job.by_period)
    logger.info('histo job done: %s %s span=%s', job.exchange, pair, job.span)


def build_histo_scheduler(config: CollectorConfig) -> BackgroundScheduler:
    """ Build an APScheduler BackgroundScheduler from a CollectorConfig.

    One interval job is registered per ``(exchange, pair)`` combination in
    ``config.histo_jobs``.  Each job runs with ``coalesce=True`` and
    ``max_instances=1`` to prevent overlapping executions.

    Parameters
    ----------
    config : CollectorConfig
        Daemon configuration.

    Returns
    -------
    apscheduler.schedulers.background.BackgroundScheduler
        Configured scheduler, not yet started.

    Examples
    --------
    >>> from dccd.daemon.config import load_config
    >>> from dccd.daemon.scheduler import build_histo_scheduler
    >>> # config = load_config('config.yml')
    >>> # scheduler = build_histo_scheduler(config)
    >>> # scheduler.start()

    """
    scheduler = BackgroundScheduler()

    for job in config.histo_jobs:
        for pair in job.pairs:
            job_id = f'{job.exchange}_{pair.replace("/", "_")}_{job.span}'
            scheduler.add_job(
                run_histo_job,
                trigger='interval',
                seconds=job.span,
                args=[job, pair, config.storage.local_path],
                id=job_id,
                name=f'{job.exchange} {pair} {job.span}s',
                coalesce=True,
                max_instances=1,
            )
            logger.debug('registered job %s', job_id)

    return scheduler


def run_once(config: CollectorConfig) -> None:
    """ Execute all histo_jobs once and return.

    Each ``(exchange, pair)`` combination is run sequentially.  A job
    failure is logged and skipped — other jobs continue regardless.

    Parameters
    ----------
    config : CollectorConfig
        Daemon configuration.

    """
    for job in config.histo_jobs:
        for pair in job.pairs:
            try:
                run_histo_job(job, pair, config.storage.local_path)
            except Exception:
                logger.exception(
                    'histo job failed: %s %s', job.exchange, pair
                )
