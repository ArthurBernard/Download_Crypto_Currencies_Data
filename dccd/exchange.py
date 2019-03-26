#!/usr/bin/env python3
# coding: utf-8


""" Base exchange class to download data.

"""

# Import built-in packages
import os
import pathlib
import time

# Import extern packages
import pandas as pd

# Import local packages
from dccd.time_tools import *


__all__ = ['ImportDataCryptoCurrencies']


class ImportDataCryptoCurrencies:
    """ Base class to import data about crypto-currencies from some
    exchanges platform. Don't use directly this class, use the respective
    class for each exchange.

    Parameters
    ----------
    path : str
        The path where data will be save.
    crypto : str
        The abreviation of the crypto-currencie.
    span : {int, 'weekly', 'daily', 'hourly'}
        If str, periodicity of observation.
        If int, number of the seconds between each observation. Minimal
        span is 60 seconds.
    platform : str
        The platform of your choice: 'Kraken', 'Poloniex'.
    fiat : str
        A fiat currency or a crypto-currency.
    form : {'xlsx', 'csv'}
        Your favorit format. Only 'xlsx' and 'csv' at the moment.

    See Also
    --------
    FromBinance, FromKraken, FromGDax, FromPoloniex

    """
    def __init__(self, path, crypto, span, platform, fiat='EUR', form='xlsx'):
        self.path = path
        self.crypto = crypto
        self.span, self.per = self._period(span)
        self.fiat = fiat
        self.pair = str(crypto + fiat)
        self.full_path = self.path + '/' + platform + '/Data/Clean_Data/'
        self.full_path += str(self.per) + '/' + self.pair
        self.last_df = pd.DataFrame()
        self.form = form

    def _get_last_date(self):
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
                print('Last saved file is in format not allowing.',
                      'Start at 1st January 2012.')
                return 1325376000

    def _set_time(self, start, end):
        """ Set the end and start in timestamp if is not yet.

        Parameters
        ----------
        start : int
            Timestamp of the first observation of you want.
        end : int
            Timestamp of the last observation of you want.

        """
        if start is 'last':
            start = self._get_last_date()
        elif isinstance(start, str):
            start = date_to_TS(start)
        else:
            pass
        if end is 'now':
            end = time.time()
        elif isinstance(end, str):
            end = date_to_TS(end)
        else:
            pass
        return int((start // self.span) * self.span), \
            int((end // self.span) * self.span)

    def _set_by_period(self, TS):
        return TS_to_date(TS, form='%' + self.by_period)

    def _name_file(self, date):
        return self.per + '_of_' + self.crypto + self.fiat + '_in_' + date

    def save(self, form='xlsx', by_period='Y'):
        """ Save data by period (default is year) in the corresponding
        format and file.
        TODO : to finish

        Parameters
        ----------
        form : {'xlsx', 'csv'}
            Format to save data.
        by_period : {'Y', 'M', 'D'}
            Period to group data in a same file. If 'Y' by year, if 'M' by
            month, if 'D' by day.

        """
        df = (self.last_df.append(self.df, sort=True)
              .drop_duplicates(subset='TS', keep='last')
              .reset_index(drop=True)
              .drop('Date', axis=1)
              .reindex(columns=[
                  'TS', 'date', 'time', 'close', 'high', 'low', 'open',
                  'quoteVolume', 'volume', 'weightedAverage'
              ]))
        pathlib.Path(self.full_path).mkdir(parents=True, exist_ok=True)
        self.by_period = by_period
        grouped = (df.set_index('TS', drop=False)
                   .groupby(self._set_by_period, axis=0))  # .reset_index()
        for name, group in grouped:
            if form is 'xlsx':
                self._excel_format(name, form, group)
            elif form is 'csv':
                group.to_csv(
                    self.full_path + '/' + self._name_file(name) + '.' + form
                )
            else:
                print('Not allowing fomat')
        return self

    def _excel_format(self, name, form, group):
        """ Save as excel format

        """
        writer = pd.ExcelWriter(
            self.full_path + '/' + self._name_file(name) + '.' + form,
            engine='xlsxwriter'
        )
        df_group = group.reset_index(drop=True)
        df_group.to_excel(
            writer, header=True, index=False, sheet_name='Sheet1'
        )
        work_book = writer.book
        work_sheet = writer.sheets['Sheet1']
        fmt = work_book.add_format(
            {'align': 'center', 'num_format': '#,##0.00'}
        )
        fmt_time = work_book.add_format(
            {'align': 'center', 'num_format': 'hh:mm:ss'}
        )
        fmt_date = work_book.add_format(
            {'align': 'center', 'num_format': 'yyyy/mm/dd'}
        )
        fmt_TS = work_book.add_format({'align': 'center'})
        work_sheet.set_column('A:A', 11, fmt_TS)
        work_sheet.set_column('B:B', 10, fmt_date)
        work_sheet.set_column('C:C', 10, fmt_time)
        work_sheet.set_column('J:J', 17, fmt)
        work_sheet.set_column('D:I', 13, fmt)
        writer.save()
        return self

    def _sort_data(self, data):
        """ Clean and sort the data.
        TODO : to finish
        """
        df = pd.DataFrame(
            data,
            index=range((self.end - self.start) // self.span + 1),
            # index=range(self.start, self.end, self.span)
        ).rename(columns={'date': 'TS'})
        TS = pd.DataFrame(
            list(range(self.start, self.end, self.span)),
            columns=['TS']
        )
        df = (df.merge(TS, on='TS', how='outer', sort=False)
              .sort_values('TS')
              .reset_index(drop=True)
              .fillna(method='pad'))
        df = df.assign(Date=pd.to_datetime(df.TS, unit='s'))
        self.df = df.assign(date=df.Date.dt.date, time=df.Date.dt.time)
        return self

    def import_data(self, start='last', end='now'):
        """ Download data from Poloniex for specific time interval.

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

    def get_data(self):
        """ Print the dataframe

        Returns
        -------
        Data : pd.DataFrame
            Current data.

        """
        return self.df

    def _period(self, span):
        if type(span) is str:
            return str_to_span(span), span
        elif type(span) is int:
            return span, span_to_str(span)
        else:
            print(
                "Error, span don't have the appropiate format",
                "as string or integer (seconds)"
            )

    def set_hierarchy(self, liste):
        """ You can determine the specific hierarchy of the files where will
        save your data.
        TODO : to finish
        """
        self.full_path = self.path
        for elt in liste:
            self.full_path += '/' + elt
