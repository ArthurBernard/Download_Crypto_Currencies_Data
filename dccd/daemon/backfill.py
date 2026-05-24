#!/usr/bin/env python3
# coding: utf-8

"""Historical OHLC backfill engine for the dccd daemon.

.. currentmodule:: dccd.daemon.backfill

Two strategies handle the fundamental difference between exchanges:

- :class:`OHLCBackfill` — exchanges whose REST endpoint accepts arbitrary
  ``start``/``end`` timestamps (Binance, Bybit, OKX, Coinbase).  A rolling
  window of ``max_candles × span`` seconds is used to stay within each
  API's page-size limit.

- :class:`KrakenBackfill` — Kraken's OHLC endpoint only returns the last
  ~720 candles regardless of ``since``.  This strategy falls back to the
  public ``/Trades`` endpoint, which supports full pagination from any
  timestamp, and resamples the raw trades into OHLC candles with pandas.

Both strategies share the same retry/progress/save loop defined in
:class:`_BackfillBase`, so all operational logic (resume from last saved
point, exponential backoff on transient errors, tqdm progress bar) lives
in one place.

Public entry points
-------------------

.. autosummary::
   :toctree: generated/

   make_job       -- build the right strategy instance for an (exchange, pair)
   run_backfill   -- run all backfill jobs from a :class:`~dccd.daemon.config.CollectorConfig`

"""

from __future__ import annotations

import time as time_mod
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pandas as pd
from tqdm import tqdm

from dccd.tools.date_time import date_to_TS

if TYPE_CHECKING:
    from dccd.daemon.config import CollectorConfig
    from dccd.histo_dl.exchange import ImportDataCryptoCurrencies

__all__ = ['OHLCBackfill', 'KrakenBackfill', 'make_job', 'run_backfill']

# ---------------------------------------------------------------------------
# Per-exchange API constraints (not user-configurable — API-level limits)
# ---------------------------------------------------------------------------

_EXCHANGE_DEFAULTS: dict[str, dict] = {
    'binance':  {'max_candles': 990, 'sleep': 0.12},
    'bybit':    {'max_candles': 990, 'sleep': 0.15},
    'kraken':   {'sleep': 1.00},
    'okx':      {'max_candles': 990, 'sleep': 0.20},
    'coinbase': {'max_candles': 300, 'sleep': 0.25},
}

# Kraken trades window: 6 h balances request count vs. memory footprint.
_KRAKEN_TRADE_WINDOW: int = 6 * 3600

_MAX_RETRIES: int = 3

DEFAULT_START: str = '2020-01-01 00:00:00'

_KRAKEN_EXCHANGE = 'kraken'


# ---------------------------------------------------------------------------
# Base class
# ---------------------------------------------------------------------------

class _BackfillBase(ABC):
    """Common retry / progress / save loop for all backfill strategies.

    Subclasses implement :meth:`_fetch_window` (fetch one window and
    populate ``self.obj.df``) and may override :meth:`window_size`,
    :meth:`_advance`, and :meth:`_dry_run_summary`.

    Parameters
    ----------
    obj : ImportDataCryptoCurrencies
        Instantiated exchange object.
    sleep : float
        Minimum seconds to wait between API calls.
    form : str
        Output format (accepted for backward compatibility; storage is
        always Parquet via :class:`~dccd.storage.DataStore`).

    """

    def __init__(
        self,
        obj: ImportDataCryptoCurrencies,
        sleep: float,
        form: str,
    ) -> None:
        self.obj = obj
        self.sleep = sleep
        self.form = form
        cls_name = type(obj).__name__[4:]  # strip leading 'From'
        self.label = f'{cls_name:8s} {obj.crypto}/{obj.fiat}'

    # ------------------------------------------------------------------
    # Abstract interface

    @property
    @abstractmethod
    def window_size(self) -> int:
        """Seconds covered by a single API request window."""

    @abstractmethod
    def _fetch_window(self, current: int, end: int) -> int:
        """Fetch one window and populate ``self.obj.df``.

        Parameters
        ----------
        current : int
            Window start (Unix timestamp, inclusive).
        end : int
            Window end (Unix timestamp, exclusive).

        Returns
        -------
        int
            Number of candles fetched (0 means nothing to save).

        """

    # ------------------------------------------------------------------
    # Optional overrides

    def _advance(self, current: int, end: int) -> int:
        """Return the start timestamp for the next window."""
        return end

    def _dry_run_summary(self, n_windows: int) -> str:
        """One-line dry-run description shown instead of fetching."""
        sleep_total = n_windows * (self.sleep + 0.5) / 60
        return (
            f'DRY RUN — {n_windows} windows, '
            f'~{sleep_total:.0f} min at {self.sleep}s/req'
        )

    # ------------------------------------------------------------------
    # Main loop

    def run(
        self,
        start_str: str,
        dry_run: bool = False,
        position: int = 0,
    ) -> None:
        """Run the full backfill for this (exchange, pair).

        Parameters
        ----------
        start_str : str
            Earliest date to backfill (``'YYYY-MM-DD HH:MM:SS'``).
            Interpreted in the timezone stored on the exchange object.
        dry_run : bool, optional
            Print an estimate without making any API calls.
        position : int, optional
            tqdm bar position (use distinct values for parallel runs).

        """
        now_ts     = int(time_mod.time())
        user_start = int(date_to_TS(start_str, tz=self.obj.tz))

        if dry_run:
            total = max(1, (now_ts - user_start) // self.window_size + 1)
            tqdm.write(f'[{self.label}] {self._dry_run_summary(total)}')
            return

        intervals = self.obj._store.missing_intervals(user_start, now_ts)

        if not intervals:
            tqdm.write(f'[{self.label}] already up to date')
            return

        def _iso(ts: int) -> str:
            return datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d')

        total        = sum(max(1, (e - s) // self.window_size + 1) for s, e in intervals)
        end_date_str = _iso(intervals[-1][1])
        bar = tqdm(
            total=total,
            desc=f'{self.label}  {_iso(intervals[0][0])} → {end_date_str}',
            unit='',
            position=position, leave=True, dynamic_ncols=True,
        )

        n_candles = 0
        skipped   = 0

        for ivl_start, ivl_end in intervals:
            current = ivl_start
            while current < ivl_end:
                end = min(current + self.window_size, ivl_end)

                last_exc: Exception | None = None
                n = 0
                for attempt in range(1, _MAX_RETRIES + 1):
                    try:
                        n = self._fetch_window(current, end)
                        break
                    except Exception as exc:
                        last_exc = exc
                        if attempt < _MAX_RETRIES:
                            tqdm.write(
                                f'[{self.label}] attempt {attempt}/{_MAX_RETRIES} '
                                f'failed: {exc} — retrying in 2s'
                            )
                            time_mod.sleep(2)
                else:
                    tqdm.write(
                        f'[{self.label}] window {current} failed after '
                        f'{_MAX_RETRIES} attempts: {last_exc} — skipping'
                    )
                    skipped += 1
                    current += self.window_size
                    bar.update(1)
                    time_mod.sleep(self.sleep)
                    continue

                if n > 0:
                    self.obj.save()
                    n_candles += n

                current_date = _iso(current)
                current = self._advance(current, end)
                bar.update(1)
                bar.set_description(f'{self.label}  {current_date} → {end_date_str}')
                bar.set_postfix(candles=n_candles, skipped=skipped)
                time_mod.sleep(self.sleep)

        bar.close()
        tqdm.write(
            f'[{self.label}] done — {n_candles:,} candles'
            + (f', {skipped} windows skipped' if skipped else '')
        )


# ---------------------------------------------------------------------------
# OHLC endpoint strategy (Binance, Bybit, OKX, Coinbase)
# ---------------------------------------------------------------------------

class OHLCBackfill(_BackfillBase):
    """Backfill strategy for exchanges with a paginated OHLC endpoint.

    Parameters
    ----------
    obj : ImportDataCryptoCurrencies
        Instantiated exchange object.
    max_candles : int
        Maximum candles per API request (exchange-specific).
    sleep : float
        Seconds to wait between requests.
    form : str
        Output format (accepted for backward compatibility).

    """

    def __init__(
        self,
        obj: ImportDataCryptoCurrencies,
        max_candles: int,
        sleep: float,
        form: str,
    ) -> None:
        super().__init__(obj, sleep, form)
        self.max_candles = max_candles

    @property
    def window_size(self) -> int:
        return self.max_candles * self.obj.span

    def _fetch_window(self, current: int, end: int) -> int:
        self.obj.import_data(start=current, end=end)
        if self.obj.df is None or self.obj.df.empty:
            return 0
        self._warn_if_suspicious(current, end)
        return len(self.obj.df)

    def _advance(self, current: int, end: int) -> int:
        return self.obj.end + self.obj.span

    def _warn_if_suspicious(self, current: int, now_ts: int) -> None:
        df = self.obj.df
        ts_min = df['TS'].min()
        tolerance = self.obj.span * 5
        if ts_min > current + tolerance:
            tqdm.write(
                f'[{self.label}] WARNING: data starts at {ts_min} but window '
                f'began at {current} (gap={ts_min - current}s > {tolerance}s)'
            )
        if df['TS'].max() > now_ts + 60:
            tqdm.write(f'[{self.label}] WARNING: future timestamps detected')


# ---------------------------------------------------------------------------
# Kraken trades strategy
# ---------------------------------------------------------------------------

class KrakenBackfill(_BackfillBase):
    """Backfill strategy for Kraken using the ``/Trades`` endpoint.

    Kraken's OHLC endpoint only returns the last ~720 candles regardless
    of ``since``.  This strategy paginates ``/Trades`` with nanosecond
    cursors, then resamples the raw trades into OHLC candles.

    Parameters
    ----------
    obj : ImportDataCryptoCurrencies
        Instantiated :class:`~dccd.histo_dl.kraken.FromKraken` object.
    sleep : float
        Seconds to wait between paginated API calls.
    form : str
        Output format (accepted for backward compatibility).

    """

    @property
    def window_size(self) -> int:
        return _KRAKEN_TRADE_WINDOW

    def _fetch_window(self, current: int, end: int) -> int:
        trades = self._paginate_trades(current, end)
        candles = self._trades_to_ohlc(trades, current, end)
        if candles:
            self.obj.start, self.obj.end = current, end
            self.obj._sort_data(candles)
        return len(candles)

    def _advance(self, current: int, end: int) -> int:
        return end

    def _dry_run_summary(self, n_windows: int) -> str:
        hours = _KRAKEN_TRADE_WINDOW // 3600
        eta_min = n_windows * 15 * self.sleep / 60
        return (
            f'DRY RUN (trades→OHLC) — {n_windows} windows '
            f'({hours}h each), ~{eta_min:.0f} min'
        )

    # ------------------------------------------------------------------
    # Private helpers

    def _paginate_trades(self, start_ts: int, end_ts: int) -> list[dict]:
        """Paginate Kraken ``/Trades`` and return raw trade dicts.

        Uses nanosecond ``since`` cursors.  Retries with exponential
        backoff on ``EGeneral:Too many requests`` errors.

        Parameters
        ----------
        start_ts, end_ts : int
            Window boundaries in Unix seconds.

        Returns
        -------
        list of dict
            Each dict has keys ``timestamp`` (float), ``price`` (float),
            ``amount`` (float).

        """
        trades: list[dict] = []
        since_ns = int(start_ts * 1e9)

        while True:
            body = self._call_with_backoff(since_ns)
            result = body['result']
            raw = result.get(self.obj.pair, [])
            last_ns = int(result.get('last', since_ns))

            for t in raw:
                ts = float(t[2])
                if ts > end_ts:
                    return trades
                trades.append({
                    'timestamp': ts,
                    'price':     float(t[0]),
                    'amount':    float(t[1]),
                })

            if not raw or last_ns >= int(end_ts * 1e9):
                break

            since_ns = last_ns
            time_mod.sleep(self.sleep)

        return trades

    def _call_with_backoff(self, since_ns: int) -> dict:
        """Call Kraken ``/Trades`` with exponential backoff on rate limits."""
        backoff = self.sleep
        for _attempt in range(6):
            r = self.obj._fetch(
                'https://api.kraken.com/0/public/Trades',
                {'pair': self.obj.pair, 'since': since_ns, 'count': 1000},
            )
            body = r.json()
            errors = body.get('error', [])
            if errors and any('Too many requests' in e for e in errors):
                tqdm.write(
                    f'[{self.label}] rate limited — backing off {backoff:.0f}s'
                )
                time_mod.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue
            if errors:
                raise RuntimeError(f'Kraken Trades API error: {errors}')
            return body
        raise RuntimeError(
            f'Kraken rate limit not resolved after retries (since={since_ns})'
        )

    def _trades_to_ohlc(
        self, trades: list[dict], start_ts: int, end_ts: int,
    ) -> list[dict]:
        """Resample raw trades into 1-min (or ``span``-s) OHLC candles.

        Empty buckets (no trades) are dropped; :meth:`_sort_data` will
        forward-fill them when building the uniform minute grid.

        Parameters
        ----------
        trades : list of dict
            Output of :meth:`_paginate_trades`.
        start_ts, end_ts : int
            Window boundaries (inclusive start, exclusive end).

        Returns
        -------
        list of dict
            Each dict contains ``date``, ``open``, ``high``, ``low``,
            ``close``, ``volume``, ``quoteVolume``, ``weightedAverage``.

        """
        if not trades:
            return []

        span = self.obj.span
        df = pd.DataFrame(trades)
        df['ts'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
        df = df.set_index('ts').sort_index()

        freq = f'{span}s'
        origin = pd.Timestamp(start_ts, unit='s', tz='UTC')
        resample_kw = {'closed': 'left', 'label': 'left', 'origin': origin}
        ohlc = df['price'].resample(freq, **resample_kw).ohlc()
        vol  = df['amount'].resample(freq, **resample_kw).sum()
        qvol = (df['price'] * df['amount']).resample(freq, **resample_kw).sum()

        result = (
            ohlc.join(vol.rename('volume')).join(qvol.rename('quoteVolume'))
            .loc[
                pd.Timestamp(start_ts, unit='s', tz='UTC'):
                pd.Timestamp(end_ts - 1, unit='s', tz='UTC'),
            ]
            .dropna(subset=['open'])
        )

        return [{
            'date':            round(row_ts.timestamp()),
            'open':            float(row['open']),
            'high':            float(row['high']),
            'low':             float(row['low']),
            'close':           float(row['close']),
            'volume':          float(row['volume']),
            'quoteVolume':     float(row['quoteVolume']),
            'weightedAverage': (
                float(row['quoteVolume'] / row['volume'])
                if row['volume'] > 0 else float(row['close'])
            ),
        } for row_ts, row in result.iterrows()]


# ---------------------------------------------------------------------------
# Factory and entry point
# ---------------------------------------------------------------------------

def make_job(
    exchange: str,
    crypto: str,
    fiat: str,
    span: int,
    path: str,
    tz: str,
    form: str,
) -> _BackfillBase:
    """Build the appropriate backfill strategy for an (exchange, pair).

    Parameters
    ----------
    exchange : str
        Exchange name (e.g. ``'binance'``).
    crypto, fiat : str
        Asset symbols (e.g. ``'BTC'``, ``'USDT'``).
    span : int
        Candle interval in seconds.
    path : str
        Root data directory.
    tz : str
        Timezone for date parsing and file labelling.
    form : str
        Output format (accepted for backward compatibility).

    Returns
    -------
    _BackfillBase
        :class:`KrakenBackfill` for Kraken, :class:`OHLCBackfill` otherwise.

    Raises
    ------
    ValueError
        If *exchange* is not in the supported defaults table.

    """
    if exchange not in _EXCHANGE_DEFAULTS:
        raise ValueError(
            f"Unsupported exchange {exchange!r}. "
            f"Supported: {sorted(_EXCHANGE_DEFAULTS)}"
        )

    defaults = _EXCHANGE_DEFAULTS[exchange]
    sleep = defaults['sleep']

    from dccd.daemon.scheduler import _HISTO_CLASSES
    cls = _HISTO_CLASSES.get(exchange)
    if cls is None:
        raise ValueError(f"No exchange class registered for {exchange!r}")

    obj = cls(path, crypto, span, fiat, form=form, tz=tz)

    if exchange == _KRAKEN_EXCHANGE:
        return KrakenBackfill(obj, sleep=sleep, form=form)

    return OHLCBackfill(
        obj,
        max_candles=defaults['max_candles'],
        sleep=sleep,
        form=form,
    )


def run_backfill(
    cfg: CollectorConfig,
    exchange: str | None = None,
    pairs: list[str] | None = None,
    start: str = DEFAULT_START,
    parallel: bool = False,
    dry_run: bool = False,
) -> None:
    """Run historical backfill for all matching jobs in *cfg*.

    Parameters
    ----------
    cfg : CollectorConfig
        Loaded daemon configuration.
    exchange : str or None, optional
        Filter to a single exchange.  ``None`` runs all.
    pairs : list of str or None, optional
        Filter to specific pairs in ``'CRYPTO/FIAT'`` format.  ``None``
        runs all pairs defined in the config.
    start : str, optional
        Earliest date to backfill.  Default is ``'2020-01-01 00:00:00'``.
    parallel : bool, optional
        Run all jobs in parallel threads.
    dry_run : bool, optional
        Print estimates without making any API calls.

    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    path = cfg.settings.data_path
    tz = cfg.settings.timezone

    jobs: list[tuple[_BackfillBase, int]] = []
    seen_paths: set[str] = set()
    for histo_job in cfg.histo_jobs:
        if exchange and histo_job.exchange != exchange:
            continue
        wanted_pairs = pairs or histo_job.pairs
        for pair in histo_job.pairs:
            if pair not in wanted_pairs:
                continue
            crypto, fiat = pair.split('/', 1)
            job = make_job(
                histo_job.exchange, crypto, fiat, histo_job.span,
                path, tz, histo_job.format,
            )
            if job.obj.full_path in seen_paths:
                tqdm.write(
                    f'[{histo_job.exchange} {pair}] skipped — resolves to the '
                    f'same data path as a previous job '
                    f'({job.obj.pair})'
                )
                continue
            seen_paths.add(job.obj.full_path)
            jobs.append((job, len(jobs)))

    if not jobs:
        tqdm.write('No matching jobs — check --exchange / --pairs filters.')
        return

    if parallel:
        with ThreadPoolExecutor(max_workers=len(jobs)) as executor:
            futures = {
                executor.submit(job.run, start, dry_run, pos): job
                for job, pos in jobs
            }
            for future in as_completed(futures):
                job = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    tqdm.write(f'[{job.label}] FATAL: {exc}')
    else:
        for job, _ in jobs:
            job.run(start, dry_run=dry_run, position=0)
