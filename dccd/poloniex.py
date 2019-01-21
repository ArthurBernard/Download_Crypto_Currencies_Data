#!/usr/bin/env python
# -*- coding: utf-8 -*-


""" 
Poloniex exchange class to download data. 

"""

# Import built-in packages
import os
import pathlib
import time

# Import extern packages
import requests
import json

# Import local packages
from dccd.time_tools import TimeTools
from dccd.exchange import ImportDataCryptoCurrencies

__all__ = ['FromPoloniex']

class FromPoloniex(ImportDataCryptoCurrencies):
    """ 
    Poloniex class to import crypto-currencies data.

    Methods
    -------
    - save : Save data by period (default is year) in the corresponding
        format and file. TO FINISH
    - get_data : Print the dataframe. 
    - set_hierarchy : You can determine the specific hierarchy of the files 
        where will save your data. TO FINISH
    - import_data : Download data since a specified date.

    Attributes
    ----------
    TO LIST
    
    """
    def __init__(self, path, crypto, span, fiat='USD', form='xlsx'):
        """ 
        Parameters
        ----------
        :path: str
            The path where data will be save.
        :crypto: str
            The abreviation of the crypto-currencie.
        :span: str ot int
            'weekly', 'daily', 'hourly', or the integer of the seconds 
            between each observations. Min 300 seconds.
        :fiat: str
            A fiat currency or a crypto-currency. Poloniex don't allow fiat 
            currencies, but USD theter.
        :form: str 
            Your favorit format. Only 'xlsx' for the moment.
        """
        if fiat in ['EUR', 'USD']:
            print("Poloniex don't allow fiat currencies, the equivalent of US dollar is Tether USD as USDT.")
            self.fiat = fiat = 'USDT'
        if crypto == 'XBT':
            crypto = 'BTC'
        ImportDataCryptoCurrencies.__init__(self, path, crypto, span, 'Poloniex', fiat, form)
        self.pair = self.fiat + '_' + crypto
        self.full_path = self.path + '/Poloniex/Data/Clean_Data/' + str(self.per) + '/' + str(self.crypto) + str(self.fiat)
        
    
    def import_data(self, start='last', end='now'):
        """ 
        Download data from Poloniex for specific time interval.
        
        :start: int or str
            Timestamp of the first observation of you want as int or date 
            format 'yyyy-mm-dd hh:mm:ss' as string.
        :end: int or str
            Timestamp of the last observation of you want as int or date 
            format 'yyyy-mm-dd hh:mm:ss' as string.
            
        """
        self.start, self.end = self._set_time(start, end)
        param = {
            'command': 'returnChartData', 
            'currencyPair': self.pair, 
            'start': self.start, 
            'end': self.end, 
            'period': self.span
        }
        r = requests.get('https://poloniex.com/public', param)
        return self._sort_data(json.loads(r.text))