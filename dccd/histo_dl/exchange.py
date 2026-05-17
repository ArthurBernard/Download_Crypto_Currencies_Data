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
import os
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
from dccd.tools.date_time import TS_to_date, date_to_TS, span_to_str, str_to_span

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

    Methods
    -------
    import_data
    save
    get_data

    """

    def __init__(self, path: str, crypto: str, span: int | str, platform: str, fiat: str = 'EUR', form: str = 'xlsx') -> None:
        """ Initialize object. """
        self.logger = logging.getLogger(__name__)
        self.path = path
        self.crypto = crypto
        self.span, self.per = self._period(span)
        self.fiat = fiat
        self.pair = str(crypto + fiat)
        self.full_path = self.path + '/' + platform + '/Data/Clean_Data/'
        self.full_path += str(self.per) + '/' + self.pair
        self.trades_path = self.path + '/' + platform + '/Data/Trades/' + self.pair
        self.orderbook_path = self.path + '/' + platform + '/Data/OrderBook/' + self.pair
        self.last_df = pd.DataFrame()
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

        Scans :attr:`full_path` for saved files and reads the last row of the
        most-recent file.  Supports ``.xlsx``, ``.csv``, and ``.parquet``
        formats.  Falls back to ``1325376000`` (2012-01-01 00:00:00 UTC) when
        the directory is empty or the file extension is not recognised.

        Returns
        -------
        int
            Unix timestamp of the last row in the latest saved file, or
            ``1325376000`` if no file is found or the format is unsupported.

        """
        pathlib.Path(self.full_path).mkdir(parents=True, exist_ok=True)

        if not os.listdir(self.full_path):
            return 1325376000

        last_file = sorted(os.listdir(self.full_path), reverse=True)[0]
        ext = last_file.rsplit('.', 1)[-1]
        full = os.path.join(self.full_path, last_file)

        if ext == 'xlsx':
            self.last_df = pd.read_excel(full)
        elif ext == 'csv':
            self.last_df = pd.read_csv(full)
        elif ext == 'parquet':
            self.last_df = pd.read_parquet(full)
        else:
            self.logger.warning(
                'Unsupported file format %s. Starting at 2012-01-01.', ext
            )
            return 1325376000

        if 'TS' in self.last_df.columns:
            return int(self.last_df['TS'].iloc[-1])

        return int(self.last_df.index[-1])

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

    def _set_by_period(self, TS: int) -> str:
        """ Convert a timestamp to a period label for grouping files.

        Parameters
        ----------
        TS : int
            Unix timestamp.

        Returns
        -------
        str
            Date string formatted according to :attr:`by_period`
            (e.g. ``'2024'`` for ``by_period='Y'``).

        """
        return TS_to_date(TS, form='%' + self.by_period)

    def _name_file(self, date: str) -> str:
        """ Build the file stem for a given period label.

        Parameters
        ----------
        date : str
            Period label returned by :meth:`_set_by_period`.

        Returns
        -------
        str
            File stem of the form ``{per}_of_{crypto}{fiat}_in_{date}``.

        """
        return self.per + '_of_' + self.crypto + self.fiat + '_in_' + date

    def save(self, form: str = 'xlsx', by_period: str = 'Y') -> ImportDataCryptoCurrencies:
        """ Save data by period (default is year) in the corresponding format
        and file.

        TODO : to finish

        Parameters
        ----------
        form : {'xlsx', 'csv'}
            Format to save data.
        by_period : {'Y', 'M', 'D'}
            - If 'Y' group data by year.
            - If 'M' group data by month.
            - If 'D' group data by day.

        """
        df = (pd.concat([self.last_df, self.df], sort=True)
              .drop_duplicates(subset='TS', keep='last')
              .reset_index(drop=True)
              .drop('Date', axis=1)
              .reindex(columns=[
                  'TS', 'date', 'time', 'close', 'high', 'low', 'open',
                  'quoteVolume', 'volume', 'weightedAverage'
              ]))
        pathlib.Path(self.full_path).mkdir(parents=True, exist_ok=True)
        self.by_period = by_period
        grouped = df.set_index('TS', drop=False).groupby(self._set_by_period)
        for name, group in grouped:
            if form == 'xlsx':
                self._excel_format(name, form, group)
            elif form == 'csv':
                group.to_csv(
                    self.full_path + '/' + self._name_file(name) + '.' + form
                )
            else:
                self.logger.warning('Not allowing format')
        return self

    def _excel_format(self, name: str, form: str, group: pd.DataFrame) -> ImportDataCryptoCurrencies:
        """ Save a grouped DataFrame slice to an Excel file.

        Parameters
        ----------
        name : str
            Period label used to build the file name via :meth:`_name_file`.
        form : str
            File extension (e.g. ``'xlsx'``).
        group : pd.DataFrame
            Slice of data for the period ``name``.

        Returns
        -------
        ImportDataCryptoCurrencies
            Returns ``self`` to allow method chaining.

        """
        path = self.full_path + '/' + self._name_file(name) + '.' + form
        df_group = group.reset_index(drop=True)
        with pd.ExcelWriter(path, engine='openpyxl') as writer:
            df_group.to_excel(
                writer, header=True, index=False, sheet_name='Sheet1'
            )
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
        df = pd.DataFrame(
            data,
            index=range((self.end - self.start) // self.span + 1),
        ).rename(columns={'date': 'TS'})
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
        self, form: str = 'csv', by_period: str = 'M'
    ) -> ImportDataCryptoCurrencies:
        """ Save :attr:`trades_df` grouped by period to :attr:`trades_path`.

        Files are named ``trades_{crypto}{fiat}_{period}.{form}``.  No
        forward-fill is applied — trades are sparse event data.

        Parameters
        ----------
        form : {'csv', 'parquet'}, optional
            Output format, default ``'csv'``.
        by_period : {'Y', 'M', 'D'}, optional
            Period label for file grouping, default ``'M'``.

        Returns
        -------
        ImportDataCryptoCurrencies
            Returns ``self`` to allow method chaining.

        """
        if self.trades_df.empty:
            return self
        pathlib.Path(self.trades_path).mkdir(parents=True, exist_ok=True)

        def _period_label(ts: float) -> str:
            return TS_to_date(int(ts), form='%' + by_period)

        grouped = self.trades_df.groupby(
            self.trades_df['TS'].map(_period_label)
        )
        for name, group in grouped:
            fname = (
                f'{self.trades_path}/trades_{self.crypto}{self.fiat}_{name}.{form}'
            )
            if form == 'parquet':
                group.to_parquet(fname, index=False)
            else:
                group.to_csv(fname, index=False)
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

    def save_orderbook(self, form: str = 'csv') -> ImportDataCryptoCurrencies:
        """ Save :attr:`orderbook_df` as a timestamped snapshot file.

        Files are named ``orderbook_{crypto}{fiat}_{unix_ts}.{form}``.

        Parameters
        ----------
        form : {'csv', 'parquet'}, optional
            Output format, default ``'csv'``.

        Returns
        -------
        ImportDataCryptoCurrencies
            Returns ``self`` to allow method chaining.

        """
        if self.orderbook_df.empty:
            return self
        pathlib.Path(self.orderbook_path).mkdir(parents=True, exist_ok=True)
        ts = int(time.time())
        fname = (
            f'{self.orderbook_path}/orderbook_{self.crypto}{self.fiat}_{ts}.{form}'
        )
        if form == 'parquet':
            self.orderbook_df.to_parquet(fname, index=False)
        else:
            self.orderbook_df.to_csv(fname, index=False)
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
