#!/usr/bin/env python
# -*- coding: utf-8 -*-


""" 
Base exchange class to download data. 

"""

# Import built-in packages
import os
import pathlib
import time

# Import extern packages
import pandas as pd

# Import local packages
from dccd.time_tools import TimeTools


__all__ = ['ImportDataCryptoCurrencies']

class ImportDataCryptoCurrencies:
    """ 
    Base class to import data about crypto-currencies from some exchanges
    platform. Don't use directly this class, use the respective class 
    for each exchange.

    Methods
    -------
    - save : Save data by period (default is year) in the corresponding
        format and file. TO FINISH
    - get_data : Print the dataframe. 
    - set_hierarchy : You can determine the specific hierarchy of the files 
        where will save your data. TO FINISH

    Attributes
    ----------
    TO LIST
    
    """
    def __init__(self, path, crypto, span, platform, fiat='EUR', form='xlsx'):
        """ 
        Parameters
        ----------
        :path: str
            The path where data will be save.
        :crypto: str
            The abreviation of the crypto-currencie.
        :span: str ot int
            'weekly', 'daily', 'hourly', or the integer of the seconds 
            between each observations.
        :platform: str
            The platform of your choice: 'Kraken', 'Poloniex'.
        :fiat: str
            A fiat currency or a crypto-currency.
        :form: str 
            Your favorit format. Only 'xlsx' for the moment.
        """
        self.tools = TimeTools()
        self.path = path
        self.crypto = crypto
        self.span, self.per = self._period(span)
        self.fiat = fiat
        self.pair = crypto + fiat
        self.full_path = self.path + '/' + platform + '/Data/Clean_Data/' + \
            str(self.per) + '/' + str(self.pair)
        self.last_df = pd.DataFrame()
        self.form = form
        
    
    def _get_last_date(self):
        """ Find the last observation imported.
        to finish
        """
        pathlib.Path(self.full_path).mkdir(parents=True, exist_ok=True)
        if not os.listdir(self.full_path):
            return 1325376000
        else: 
            last_file = sorted(os.listdir(self.full_path), reverse=True)[0]
            if last_file.split('.')[-1] == 'xlsx':
                self.last_df = pd.read_excel(self.full_path + '/' + str(last_file))
                return self.last_df.TS.iloc[-1]
            else:
                print('Last saved file is in format not allowing. Start at',
                    '1st January 2012.')
                return 1325376000
        
    
    def _set_time(self, start, end):
        """ 
        Set the end and start in timestamp if is not yet.

        Parameters
        ----------
        :start: int
            Timestamp of the first observation of you want.
        :end: int 
            Timestamp of the last observation of you want.
        """
        if start is 'last':
            start = self._get_last_date()
        elif isinstance(start, str):
            start = self.tools.date_to_TS(start)
        else:
            pass
        if end is 'now':
            end = time.time()
        elif isinstance(end, str):
            end = self.tools.date_to_TS(end)
        else:
            pass
        return int((start // self.span) * self.span), \
            int((end // self.span) * self.span)
        
    
    def _set_by_period(self, TS):
        return self.tools.TS_to_date(TS, form='%' + self.by_period)
        
    
    def _name_file(self, date):
        return self.per + '_of_' + self.crypto + self.fiat + '_in_' + date
        
    
    def save(self, form='xlsx', by_period='Y'):
        """ Save data by period (default is year) in the corresponding
        format and file.
        to finish
        """
        df = (self.last_df.append(self.df, sort=True)
              .drop_duplicates(subset='TS', keep='last')
              .reset_index(drop=True)
              .drop('Date', axis=1)
              .reindex(columns = ['TS', 'date', 'time', 'close', 'high', 'low', 'open', 
                                  'quoteVolume', 'volume', 'weightedAverage']))
        pathlib.Path(self.full_path).mkdir(parents=True, exist_ok=True) 
        self.by_period = by_period
        grouped = df.set_index('TS', drop=False).groupby(self._set_by_period, axis=0)#.reset_index()
        for name, group in grouped:
            if form is 'xlsx':
                writer = pd.ExcelWriter(self.full_path + '/' + self._name_file(name) + '.' + form, engine='xlsxwriter')
                df_group = group.reset_index(drop=True)
                df_group.to_excel(writer, header=True, index=False, sheet_name='Sheet1')
                work_book = writer.book
                work_sheet = writer.sheets['Sheet1']
                fmt = work_book.add_format({'align': 'center', 'num_format': '#,##0.00'})
                fmt_time = work_book.add_format({'align': 'center', 'num_format': 'hh:mm:ss'})
                fmt_date = work_book.add_format({'align': 'center', 'num_format': 'yyyy/mm/dd'})
                fmt_TS = work_book.add_format({'align': 'center'})
                work_sheet.set_column('A:A', 11, fmt_TS)
                work_sheet.set_column('B:B', 10, fmt_date)
                work_sheet.set_column('C:C', 10, fmt_time)
                work_sheet.set_column('J:J', 17, fmt)
                work_sheet.set_column('D:I', 13, fmt)
                writer.save()
            else:
                print('Not allowing fomat')
        return self
        
    
    def _sort_data(self, data):
        """ Clean and sort the data.
        to finish
        """
        df = pd.DataFrame(data).rename(columns = {'date': 'TS'})#, index = range(self.start, self.end, self.span))
        TS = pd.DataFrame(list(range(self.start, self.end, self.span)), columns = ['TS'])
        df = (df.merge(TS, on='TS', how='outer', sort=False)
              .sort_values('TS')
              .reset_index(drop=True)
              .fillna(method='pad'))
        df = df.assign(Date = pd.to_datetime(df.TS, unit='s'))
        self.df = df.assign(date=df.Date.dt.date, time=df.Date.dt.time)
        return self
        
    
    def get_data(self):
        """ Print the dataframe
        
        """
        return self.df
        
    
    def _period(self, span):
        if type(span) is str:
            return self.tools.str_to_TS(span), span
        elif type(span) is int:
            return span, self.tools.TS_to_str(span)
        else:
            print("Error, span don't have the appropiate format as string or integer (seconds)")
        
    
    def set_hierarchy(self, liste):
        """ You can determine the specific hierarchy of the files where will save your data.
        to finish
        """
        self.full_path = self.path
        for elt in liste:
            self.full_path += '/' + elt