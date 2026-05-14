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
The following object is shapped to download data from crypto-currency exchanges
(currently only Binance, Coinbase, Kraken).

"""

from __future__ import annotations

# Import built-in packages
import logging
import os
import pathlib
import time
from typing import TYPE_CHECKING, Any

# Import extern packages
import pandas as pd
import requests
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

# Import local packages
from dccd.models import OHLCBar
from dccd.tools.date_time import TS_to_date, date_to_TS, span_to_str, str_to_span

if TYPE_CHECKING:
    import polars as pl

__all__ = ['ImportDataCryptoCurrencies']


def _should_retry(exc):
    return (isinstance(exc, requests.HTTPError)
            and exc.response.status_code == 429)


class ImportDataCryptoCurrencies:
    """ Base class to import data about crypto-currencies from some exchanges.

    Parameters
    ----------
    path : str
        The path where data will be save.
    crypto : str
        The abreviation of the crypto-currencie.
    span : {int, 'weekly', 'daily', 'hourly'}
        - If str, periodicity of observation.
        - If int, number of the seconds between each observation, minimal span\
            is 60 seconds.
    platform : str
        The platform of your choice: 'Kraken', 'Coinbase'.
    fiat : str
        A fiat currency or a crypto-currency.
    form : {'xlsx', 'csv'}
        Your favorit format. Only 'xlsx' and 'csv' at the moment.

    Notes
    -----
    Don't use directly this class, use the respective class for each exchange.

    See Also
    --------
    FromBinance, FromKraken, FromCoinbase

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
        self.last_df = pd.DataFrame()
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
        """ Find the last observation imported.

        TODO : to finish
        """
        pathlib.Path(self.full_path).mkdir(parents=True, exist_ok=True)

        if not os.listdir(self.full_path):

            return 1325376000

        else:
            last_file = sorted(os.listdir(self.full_path), reverse=True)[0]
            if last_file.split('.')[-1] == 'xlsx':
                self.last_df = pd.read_excel(
                    self.full_path + '/' + str(last_file)
                )

                return self.last_df.TS.iloc[-1]

            else:
                self.logger.warning(
                    'Last saved file is in format not allowing. '
                    'Start at 1st January 2012.'
                )

                return 1325376000

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
        return TS_to_date(TS, form='%' + self.by_period)

    def _name_file(self, date: str) -> str:
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
        """ Save as excel format. """
        path = self.full_path + '/' + self._name_file(name) + '.' + form
        df_group = group.reset_index(drop=True)
        with pd.ExcelWriter(path, engine='openpyxl') as writer:
            df_group.to_excel(
                writer, header=True, index=False, sheet_name='Sheet1'
            )
        return self

    def _sort_data(self, data: list[dict[str, Any]]) -> ImportDataCryptoCurrencies:
        """ Clean and sort the data.

        TODO : to finish
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

    def _import_data(self, start: int | str, end: int | str) -> list[dict[str, Any]]:
        """ Fetch raw data from the exchange (implemented by subclasses). """
        raise NotImplementedError

    def set_hierarchy(self, liste: list[str]) -> None:
        """ Set the specific hierarchy of the files where will save your data.

        TODO : to finish
        """
        self.full_path = self.path
        for elt in liste:
            self.full_path += '/' + elt
