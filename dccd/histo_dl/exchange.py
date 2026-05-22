#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2019-08-30 09:25:01
# @Last modified by: ArthurBernard
# @Last modified time: 2026-05-12

""" Base object to download historical data from REST API.

Notes
-----
The following object is shaped to download data from crypto-currency exchanges
(Binance, Coinbase, Kraken, Bybit, OKX).

"""

from __future__ import annotations

# Import built-in packages
import logging
import pathlib
import time
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

# Import extern packages
import pandas as pd
import requests
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

# Import local packages
from dccd.models import OHLCBar, OrderBookEntry, Trade
from dccd.storage import DataStore
from dccd.tools.date_time import date_to_TS, span_to_str, str_to_span

if TYPE_CHECKING:
    import polars as pl

__all__ = ['ImportDataCryptoCurrencies']


def _should_retry(exc):
    return (isinstance(exc, requests.HTTPError)
            and exc.response.status_code == 429)


class ImportDataCryptoCurrencies(ABC):
    """ Base class to import data about crypto-currencies from some exchanges.

    Parameters
    ----------
    path : str
        The path where data will be saved.
    crypto : str
        The abbreviation of the crypto-currency.
    span : {int, 'weekly', 'daily', 'hourly'}
        - If str, periodicity of observation.
        - If int, number of the seconds between each observation, minimal span\
            is 60 seconds.
    platform : str
        The platform of your choice: 'Binance', 'Kraken', 'Coinbase',
        'Bybit', 'OKX'.
    fiat : str
        A fiat currency or a crypto-currency.
    form : {'xlsx', 'csv'}
        Your favorite format. Only 'xlsx' and 'csv' at the moment.

    Notes
    -----
    Don't use directly this class, use the respective class for each exchange.

    See Also
    --------
    FromBinance, FromKraken, FromCoinbase, FromBybit, FromOKX

    Attributes
    ----------
    pair : str
        Pair symbol, `crypto + fiat`.
    start, end : int
        Timestamp to starting and ending download data.
    span : int
        Number of seconds between observations.
    full_path : str
        Path to save data.
    form : str
        Format to save data.
    trades_df : pd.DataFrame
        Trades data after calling :meth:`import_trades`.
    orderbook_df : pd.DataFrame
        Order book snapshot after calling :meth:`import_orderbook`.

    Methods
    -------
    import_data
    save
    get_data
    import_trades
    save_trades
    import_orderbook
    save_orderbook

    """

    def __init__(self, path: str, crypto: str, span: int | str, platform: str, fiat: str = 'EUR', form: str = 'xlsx', tz: str = 'local') -> None:
        """ Initialize object. """
        self.logger = logging.getLogger(__name__)
        self.path = path
        self.crypto = crypto
        self.span, self.per = self._period(span)
        self.fiat = fiat
        self.tz = tz
        self.pair = str(crypto + fiat)
        self._exchange_name = platform.lower()
        self._canonical_pair = f'{crypto}/{fiat}'
        self._store = DataStore(path, self._exchange_name, self._canonical_pair, self.span, 'ohlc')
        self.full_path = str(self._store.directory)
        self.trades_df: pd.DataFrame = pd.DataFrame()
        self.orderbook_df: pd.DataFrame = pd.DataFrame()
        self.form = form
        self.start: int = 0
        self.end: int = 0

    @retry(retry=retry_if_exception(_should_retry),
           wait=wait_exponential(multiplier=1, min=1, max=60),
           stop=stop_after_attempt(5))
    def _fetch(self, url: str, params: dict[str, Any]) -> requests.Response:
        """ Fetch URL with automatic retry on HTTP 429. """
        r = requests.get(url, params)
        if r.status_code == 429:
            r.raise_for_status()
        return r

    def _get_last_date(self) -> int:
        """ Find the timestamp of the last imported observation.

        Delegates to :meth:`~dccd.storage.DataStore.last_timestamp`.
        Falls back to ``1325376000`` (2012-01-01 00:00:00 UTC) when no data
        has been saved yet.

        Returns
        -------
        int
            Unix timestamp of the last saved row, or ``1325376000`` if no
            file is found.

        """
        ts = self._store.last_timestamp()
        return ts if ts is not None else 1325376000

    def _set_time(self, start: int | str, end: int | str) -> tuple[int, int]:
        """ Set the end and start in timestamp if is not yet.

        Parameters
        ----------
        start : int
            Timestamp of the first observation of you want.
        end : int
            Timestamp of the last observation of you want.

        """
        _start: int | float
        _end: int | float

        if start == 'last':
            _start = self._get_last_date()
        elif isinstance(start, str):
            _start = date_to_TS(start)
        else:
            _start = start

        if end == 'now':
            _end = time.time()
        elif isinstance(end, str):
            _end = date_to_TS(end)
        else:
            _end = end

        return int((_start // self.span) * self.span), \
            int((_end // self.span) * self.span)

    def save(self, form: str = 'parquet', by_period: str = 'Y') -> ImportDataCryptoCurrencies:
        """ Save :attr:`df` to disk via :class:`~dccd.storage.DataStore`.

        Data is always written as Parquet, grouped annually.  The *form* and
        *by_period* parameters are accepted for backward compatibility but
        ignored — storage format and period granularity are managed by
        :class:`~dccd.storage.DataStore`.

        Parameters
        ----------
        form : str, optional
            Ignored.  Kept for backward-compatibility.
        by_period : str, optional
            Ignored.  Kept for backward-compatibility.

        """
        df = self.df.copy()
        if 'Date' in df.columns:
            df = df.drop('Date', axis=1)
        self._store.save(df)
        self.full_path = str(self._store.directory)
        return self

    def _sort_data(self, data: list[dict[str, Any]]) -> ImportDataCryptoCurrencies:
        """ Validate, merge, and sort raw OHLCV data against :attr:`last_df`.

        Validates each record through :class:`~dccd.models.OHLCBar`, builds a
        complete timestamp grid from ``self.start`` to ``self.end``, outer-merges
        with new data, forward-fills gaps, and stores the result in
        :attr:`df`.

        Parameters
        ----------
        data : list of dict
            Raw OHLCV records as returned by :meth:`_import_data`.  Each dict
            must contain at least the keys expected by
            :class:`~dccd.models.OHLCBar`.

        Returns
        -------
        ImportDataCryptoCurrencies
            Returns ``self`` to allow method chaining.

        """
        data = [OHLCBar(**d).model_dump(exclude_none=False) for d in data]
        df = pd.DataFrame(data).rename(columns={'date': 'TS'})
        # Use self.end as the exclusive grid boundary so the full window is
        # covered even when the last trade arrives >span seconds before the
        # window end.  Callers must set self.end to the correct window
        # boundary before calling _sort_data (the backfill scripts do this).
        TS = pd.DataFrame(
            list(range(self.start, self.end, self.span)),
            columns=['TS']
        )
        df = (df.merge(TS, on='TS', how='outer', sort=False)
              .sort_values('TS')
              .reset_index(drop=True)
              .ffill())
        df = df.assign(Date=pd.to_datetime(df.TS, unit='s'))
        self.df = df.assign(date=df.Date.dt.date, time=df.Date.dt.time)
        # Update self.end to the actual last candle so callers can advance
        # their window pointer correctly (critical for Kraken).
        if not df.empty:
            self.end = int(self.df['TS'].max())
        return self

    def import_data(self, start: int | str = 'last', end: int | str = 'now') -> ImportDataCryptoCurrencies:
        """ Download data for specific time interval.

        Parameters
        ----------
        start : int or str
            Timestamp of the first observation of you want as int or date
            format 'yyyy-mm-dd hh:mm:ss' as string.
        end : int or str /! NOT ALLOWED TO KRAKEN EXCHANGE /!
            Timestamp of the last observation of you want as int or date
            format 'yyyy-mm-dd hh:mm:ss' as string.

        Returns
        -------
        data : pd.DataFrame
            Data sorted and cleaned in a data frame.

        """
        data = self._import_data(start=start, end=end)

        return self._sort_data(data)

    def get_data(self, format: str = 'pandas') -> pd.DataFrame | pl.DataFrame:
        """ Return the downloaded data.

        Parameters
        ----------
        format : {'pandas', 'polars'}, optional
            Output format. Default is 'pandas'.

        Returns
        -------
        pandas.DataFrame or polars.DataFrame
            Current data in the requested format.

        """
        if format == 'polars':
            import polars as pl
            return pl.from_pandas(self.df)
        return self.df

    def _period(self, span: int | str) -> tuple[int, str]:
        if type(span) is str:
            seconds = str_to_span(span)
            if seconds is None:
                raise ValueError(f"Unknown span string: {span!r}")
            return seconds, span
        elif type(span) is int:
            label = span_to_str(span)
            if label is None:
                raise ValueError(f"Unknown span value: {span}")
            return span, label
        else:
            raise TypeError("span must be str or int")

    @abstractmethod
    def _import_data(self, start: int | str, end: int | str) -> list[dict[str, Any]]:
        """ Fetch raw data from the exchange (implemented by subclasses). """

    # ------------------------------------------------------------------
    # Trades — public API
    # ------------------------------------------------------------------

    def import_trades(
        self, start: int | str = 0, end: int | str = 'now'
    ) -> ImportDataCryptoCurrencies:
        """ Fetch individual trades for a time window.

        Downloads executed trades from the exchange REST API, validates each
        record, and stores the result in :attr:`trades_df`.  Use
        :meth:`save_trades` to persist to disk.

        Parameters
        ----------
        start : int or str, optional
            Start of the time window.  Accepts a Unix timestamp (int), a date
            string ``'yyyy-mm-dd hh:mm:ss'``, or ``0`` (default, meaning "as
            far back as the API allows").
        end : int or str, optional
            End of the time window.  ``'now'`` (default) resolves to the
            current UTC time.  Accepts a Unix timestamp or date string.

        Returns
        -------
        ImportDataCryptoCurrencies
            Returns ``self`` for method chaining.

        Notes
        -----
        Exchanges vary in how much history they expose:

        - **Binance** and **Kraken** provide full paginated history.
        - **OKX** exposes several months of history via cursor pagination.
        - **Bybit** returns the ~1 000 most recent trades regardless of
          ``start``/``end``.
        - **Coinbase** returns up to 100 recent trades (cursor-based,
          no deep history).

        """
        _start: int | float = date_to_TS(start) if isinstance(start, str) else start
        if end == 'now':
            _end: int | float = time.time()
        elif isinstance(end, str):
            _end = date_to_TS(end)
        else:
            _end = end
        data = self._import_trades(int(_start), int(_end))
        return self._sort_trades(data)

    def _import_trades(self, start: int, end: int) -> list[dict[str, Any]]:
        """ Fetch raw trades from the exchange (override in subclasses).

        Parameters
        ----------
        start : int
            Start Unix timestamp (seconds).
        end : int
            End Unix timestamp (seconds).

        Returns
        -------
        list of dict
            Each dict must contain: ``tid``, ``timestamp``, ``price``,
            ``amount``, ``type``.

        Raises
        ------
        NotImplementedError
            If the subclass has not implemented this method.

        """
        raise NotImplementedError(
            f'{type(self).__name__} does not implement _import_trades'
        )

    def _sort_trades(self, data: list[dict[str, Any]]) -> ImportDataCryptoCurrencies:
        """ Validate, sort, and deduplicate raw trade records.

        Parameters
        ----------
        data : list of dict
            Raw trade records as returned by :meth:`_import_trades`.

        Returns
        -------
        ImportDataCryptoCurrencies
            Returns ``self`` to allow method chaining.

        """
        validated = [Trade(**d).model_dump() for d in data]
        df = pd.DataFrame(validated).rename(columns={'timestamp': 'TS'})
        df = df.sort_values('TS').reset_index(drop=True)
        if not df.empty and df['tid'].notna().any():
            df = df.drop_duplicates(subset='tid', keep='last').reset_index(drop=True)
        self.trades_df = df
        return self

    def save_trades(
        self, form: str = 'parquet', by_period: str = 'D'
    ) -> ImportDataCryptoCurrencies:
        """ Save :attr:`trades_df` via :class:`~dccd.storage.DataStore`.

        Trades are grouped by calendar day and written as Parquet.  The *form*
        and *by_period* parameters are accepted for backward compatibility but
        ignored.

        Parameters
        ----------
        form : str, optional
            Ignored.  Kept for backward-compatibility.
        by_period : str, optional
            Ignored.  Kept for backward-compatibility.

        Returns
        -------
        ImportDataCryptoCurrencies
            Returns ``self`` to allow method chaining.

        """
        if self.trades_df.empty:
            return self
        store = DataStore(self.path, self._exchange_name, self._canonical_pair, None, 'trades')
        store.save(self.trades_df)
        return self

    # ------------------------------------------------------------------
    # Order book — public API
    # ------------------------------------------------------------------

    def import_orderbook(self, depth: int = 50) -> ImportDataCryptoCurrencies:
        """ Fetch the current order book snapshot at a given depth.

        Downloads the bid/ask ladder from the exchange REST API, validates
        each level, and stores the result in :attr:`orderbook_df`.  Use
        :meth:`save_orderbook` to persist to disk.

        Parameters
        ----------
        depth : int, optional
            Number of price levels to fetch per side (bids + asks), default
            50.  Maximum varies by exchange.

        Returns
        -------
        ImportDataCryptoCurrencies
            Returns ``self`` for method chaining.

        Notes
        -----
        Order book REST endpoints return a **current snapshot** only.
        Historical order book data is not available via public APIs.

        """
        data = self._import_orderbook(depth)
        return self._sort_orderbook(data)

    def _import_orderbook(self, depth: int) -> list[dict[str, Any]]:
        """ Fetch the raw order book from the exchange (override in subclasses).

        Parameters
        ----------
        depth : int
            Number of price levels per side.

        Returns
        -------
        list of dict
            Each dict must contain: ``side``, ``price``, ``amount``, ``count``.

        Raises
        ------
        NotImplementedError
            If the subclass has not implemented this method.

        """
        raise NotImplementedError(
            f'{type(self).__name__} does not implement _import_orderbook'
        )

    def _sort_orderbook(self, data: list[dict[str, Any]]) -> ImportDataCryptoCurrencies:
        """ Validate and sort raw order book levels.

        Bids are sorted descending by price; asks ascending.

        Parameters
        ----------
        data : list of dict
            Raw order book levels as returned by :meth:`_import_orderbook`.

        Returns
        -------
        ImportDataCryptoCurrencies
            Returns ``self`` to allow method chaining.

        """
        validated = [OrderBookEntry(**d).model_dump() for d in data]
        df = pd.DataFrame(validated)
        df['_p'] = df['price'].astype(float)
        bids = df[df['side'] == 'bid'].sort_values('_p', ascending=False)
        asks = df[df['side'] == 'ask'].sort_values('_p', ascending=True)
        self.orderbook_df = (
            pd.concat([bids, asks]).drop('_p', axis=1).reset_index(drop=True)
        )
        return self

    def save_orderbook(self, form: str = 'parquet') -> ImportDataCryptoCurrencies:
        """ Save :attr:`orderbook_df` via :class:`~dccd.storage.DataStore`.

        The snapshot is timestamped with the current UTC time and written into
        the daily orderbook file.  The *form* parameter is accepted for
        backward compatibility but ignored.

        Parameters
        ----------
        form : str, optional
            Ignored.  Kept for backward-compatibility.

        Returns
        -------
        ImportDataCryptoCurrencies
            Returns ``self`` to allow method chaining.

        """
        if self.orderbook_df.empty:
            return self
        df = self.orderbook_df.copy()
        if 'TS' not in df.columns:
            df.insert(0, 'TS', int(time.time()))
        store = DataStore(self.path, self._exchange_name, self._canonical_pair, None, 'orderbook')
        store.save(df)
        return self

    # ------------------------------------------------------------------

    def set_hierarchy(self, liste: list[str]) -> None:
        """ Override the default save path with a custom directory hierarchy.

        Rebuilds :attr:`full_path` by joining :attr:`path` with each element
        in ``liste``.  Call this before :meth:`import_data` if you want to
        store files in a non-standard directory layout.

        Parameters
        ----------
        liste : list of str
            Path components to append to :attr:`path`.

        """
        self.full_path = self.path
        for elt in liste:
            self.full_path += '/' + elt
        # Reset the store directory cache so the next save uses full_path.
        self._store._dir = pathlib.Path(self.full_path)
