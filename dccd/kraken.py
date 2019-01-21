#!/usr/bin/env python
# -*- coding: utf-8 -*-


""" 
Kraken exchange class to download data. 

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

__all__ = ['FromKraken']

class FromKraken(ImportDataCryptoCurrencies):
    """ 
    Kraken class to import crypto-currencies data.

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
            between each observations. Min 60 seconds.
        :fiat: str
            A fiat currency or a crypto-currency.
        :form: str 
            Your favorit format. Only 'xlsx' for the moment.
        """
        ImportDataCryptoCurrencies.__init__(self, path, crypto, span, 'Kraken', fiat=fiat, form=form)
        if crypto == 'BTC':
            crypto = 'XBT'
        if crypto == 'BCH' or crypto == 'DASH':
            self.pair = crypto+fiat
        elif fiat not in ['EUR', 'USD', 'CAD', 'JPY', 'GBP']:
            self.pair = 'X'+crypto+'X'+fiat
        else:
            self.pair = 'X'+crypto+'Z'+fiat
        
    
    def import_data(self, start='last'):
        """ 
        Download data from Kraken since a specific time until now.
        
        Parameters
        ----------
        :start: int or str
            Timestamp of the first observation of you want as int or date 
            format 'yyyy-mm-dd hh:mm:ss' as string.
        
        """
        self.start, self.end = self._set_time(start, time.time())
        param = {
            'pair': self.pair, 
            'interval': int(self.span / 60), 
            'since': self.start - self.span
        }
        r = requests.get('https://api.kraken.com/0/public/OHLC', param)
        text = json.loads(r.text)['result'][self.pair]
        data = [{'date': float(e[0]), 'open': float(e[1]), 'high': float(e[2]), 
                 'low': float(e[3]), 'close': float(e[4]), 
                 'weightedAverage': float(e[5]), 'volume': float(e[6]), 
                 'quoteVolume': float(e[6])*float(e[5])} for e in text]
        return self._sort_data(data)