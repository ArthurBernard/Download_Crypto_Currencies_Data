#!/usr/bin/env python3
# coding: utf-8

""" Health monitoring for the dccd daemon.

Tracks per-job metrics (last run, last success, rows collected, error count),
persists them to a JSON file, configures a rotating log handler, and sends
optional webhook alerts when consecutive failures exceed the configured
threshold.

"""

from __future__ import annotations

import json
import logging
import time
import urllib.request
from dataclasses import asdict, dataclass
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dccd.daemon.config import AlertConfig

__all__ = ['HealthMonitor', 'JobMetrics']

logger = logging.getLogger(__name__)

_LOG_MAX_BYTES = 10 * 1024 * 1024  # 10 MB
_LOG_BACKUP_COUNT = 5


@dataclass
class JobMetrics:
    """ Per-job health metrics, updated after every execution attempt.

    ``errors_count`` is a *consecutive* failure counter: it resets to 0
    whenever :meth:`HealthMonitor.record_success` is called, so it reflects
    the current streak of failures rather than a lifetime total.

    Parameters
    ----------
    last_run_at : float or None
        Unix timestamp of the most recent execution attempt (success or failure).
    last_success_at : float or None
        Unix timestamp of the most recent successful execution.
        ``None`` until the first success.
    rows_collected : int
        Cumulative number of rows saved across all successful runs.
    errors_count : int
        Number of *consecutive* failures since the last success.
        Resets to 0 on the next successful run.

    """

    last_run_at: float | None = None
    last_success_at: float | None = None
    rows_collected: int = 0
    errors_count: int = 0


class HealthMonitor:
    """ Monitor job health, persist metrics, and send webhook alerts.

    ``HealthMonitor`` serves three purposes:

    1. **Rotating log** — attaches a :class:`~logging.handlers.RotatingFileHandler`
       (10 MB × 5 backups) to the *root* logger on construction, so every
       ``logging`` call anywhere in the process lands in
       ``{local_path}/.dccd/dccd.log`` in addition to the console.
    2. **Per-job metrics** — :meth:`record_success` / :meth:`record_failure`
       update a :class:`JobMetrics` entry for each ``(exchange, pair)`` key and
       flush the full metrics dict to ``{local_path}/.dccd/metrics.json`` after
       each call.  The JSON file is reloaded on startup, so metrics survive
       daemon restarts.
    3. **Webhook alerts** — when ``errors_count`` reaches
       ``alerts.max_consecutive_errors``, a JSON POST is sent to
       ``alerts.webhook_url`` (Slack / Discord / generic).  Alerting is
       completely optional: pass ``AlertConfig()`` with no ``webhook_url`` to
       disable it.

    Use this class directly when embedding the scheduler in your own process
    (see :func:`~dccd.daemon.scheduler.run_once` and
    :func:`~dccd.daemon.scheduler.build_histo_scheduler`).  The ``dccd``
    CLI commands instantiate it automatically.

    Parameters
    ----------
    local_path : str or Path
        Root data directory (``CollectorConfig.storage.local_path``).
        The hidden directory ``{local_path}/.dccd/`` is created on init if it
        does not exist.
    alerts : AlertConfig
        Alerting configuration (webhook URL and error threshold).

    Notes
    -----
    The rotating log handler is added to the **root** logger, not to the
    module logger.  All loggers in the process therefore write to the file
    after :class:`HealthMonitor` is constructed — this is intentional so that
    APScheduler, WebSocket, and application logs are all captured together.

    Calling :meth:`record_failure` does *not* suppress or re-raise the
    original exception.  The caller is responsible for exception handling;
    ``HealthMonitor`` only observes the outcome.

    Examples
    --------
    Standalone usage inside a custom scheduler loop:

    >>> from dccd.daemon.config import AlertConfig
    >>> from dccd.daemon.health import HealthMonitor
    >>> import tempfile, pathlib
    >>> with tempfile.TemporaryDirectory() as tmp:
    ...     alerts = AlertConfig()           # no webhook
    ...     monitor = HealthMonitor(tmp, alerts)
    ...     monitor.record_success('binance', 'BTC/USDT', rows=120)
    ...     monitor.record_success('binance', 'BTC/USDT', rows=95)
    ...     monitor.record_failure('kraken',  'ETH/USD')
    ...     m = monitor.get_metrics()
    ...     print(m['binance/BTC/USDT'].rows_collected)
    ...     print(m['kraken/ETH/USD'].errors_count)
    215
    1

    """

    def __init__(self, local_path: str | Path, alerts: AlertConfig) -> None:
        self._dir = Path(local_path) / '.dccd'
        self._dir.mkdir(parents=True, exist_ok=True)
        self._metrics_file = self._dir / 'metrics.json'
        self._alerts = alerts
        self._metrics: dict[str, JobMetrics] = {}
        self._load_metrics()
        self._setup_logging()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def record_success(self, exchange: str, pair: str, rows: int = 0) -> None:
        """ Record a successful job execution.

        Parameters
        ----------
        exchange : str
            Exchange name (e.g. ``'binance'``).
        pair : str
            Trading pair (e.g. ``'BTC/USDT'``).
        rows : int, optional
            Number of data rows collected, default 0.

        """
        key = self._key(exchange, pair)
        m = self._metrics.setdefault(key, JobMetrics())
        now = time.time()
        m.last_run_at = now
        m.last_success_at = now
        m.rows_collected += rows
        m.errors_count = 0
        self._save_metrics()
        logger.debug('health: success %s %s rows=%d', exchange, pair, rows)

    def record_failure(self, exchange: str, pair: str) -> None:
        """ Record a failed job execution.

        Parameters
        ----------
        exchange : str
            Exchange name.
        pair : str
            Trading pair.

        """
        key = self._key(exchange, pair)
        m = self._metrics.setdefault(key, JobMetrics())
        m.last_run_at = time.time()
        m.errors_count += 1
        self._save_metrics()
        logger.warning('health: failure %s %s errors=%d', exchange, pair, m.errors_count)
        if (self._alerts.webhook_url
                and m.errors_count >= self._alerts.max_consecutive_errors):
            self._send_alert(exchange, pair, m.errors_count)

    def get_metrics(self) -> dict[str, JobMetrics]:
        """ Return a snapshot of the current metrics dict.

        Returns
        -------
        dict of str to JobMetrics
            Keys are ``'{exchange}/{pair}'`` strings.

        """
        return dict(self._metrics)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _key(exchange: str, pair: str) -> str:
        return f'{exchange}/{pair}'

    def _send_alert(self, exchange: str, pair: str, errors_count: int) -> None:
        text = (
            f':warning: dccd: {errors_count} consecutive errors '
            f'on {exchange} {pair}'
        )
        payload = json.dumps({'text': text}).encode()
        req = urllib.request.Request(
            self._alerts.webhook_url,  # type: ignore[arg-type]
            data=payload,
            headers={'Content-Type': 'application/json'},
        )
        try:
            urllib.request.urlopen(req, timeout=5)
            logger.info('health: alert sent for %s %s', exchange, pair)
        except Exception:
            logger.exception('health: failed to send alert for %s %s', exchange, pair)

    def _save_metrics(self) -> None:
        data = {k: asdict(v) for k, v in self._metrics.items()}
        self._metrics_file.write_text(json.dumps(data, indent=2))

    def _load_metrics(self) -> None:
        if self._metrics_file.exists():
            try:
                data = json.loads(self._metrics_file.read_text())
                self._metrics = {k: JobMetrics(**v) for k, v in data.items()}
            except Exception:
                logger.warning('health: could not load metrics from %s', self._metrics_file)

    def _setup_logging(self) -> None:
        log_file = self._dir / 'dccd.log'
        handler = RotatingFileHandler(
            log_file,
            maxBytes=_LOG_MAX_BYTES,
            backupCount=_LOG_BACKUP_COUNT,
        )
        handler.setFormatter(
            logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
        )
        logging.getLogger().addHandler(handler)
