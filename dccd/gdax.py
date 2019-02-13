#!/usr/bin/env python
# -*- coding: utf-8 -*-


""" 
GDax exchange class to download data. 

"""

# Import built-in packages
import os
import pathlib
import time

# Import extern packages
import requests
import json

# Import local packages
from dccd.time_tools import *
from dccd.exchange import ImportDataCryptoCurrencies

__all__ = ['FromGDax']

class FromGDax(ImportDataCryptoCurrencies):
    """ Class to import crypto-currencies data from the GDax exchange.
    
    Parameters
    ----------
    path : str
        The path where data will be save.
    crypto : str
        The abreviation of the crypto-currency.
    span : {int, 'weekly', 'daily', 'hourly'}
        If str, periodicity of observation. 
        If int, number of the seconds between each observation. Minimal 
        span is 60 seconds.
    fiat : str
        A fiat currency or a crypto-currency.
    form : {'xlsx', 'csv'}
        Your favorit format. Only 'xlsx' and 'csv' for the moment.

    See Also
    --------
    FromBinance, FromKraken, FromPoloniex

    Notes
    -----
    See GDax API documentation [1]_ for more details on parameters.

    References
    ----------
    .. [1] https://docs.pro.coinbase.com/
    
    """
    def __init__(self, path, crypto, span, fiat='USD', form='xlsx'):
        if crypto is 'XBT':
            crypto = 'BTC'
        ImportDataCryptoCurrencies.__init__(
            self, path, crypto, span, 'GDAX', fiat, form
        )
        self.pair = crypto + '-' + fiat
        self.full_path = self.path + '/GDAX/Data/Clean_Data/' 
        self.full_path += str(self.per) + '/' + str(self.crypto) + str(self.fiat)
        
    
    def _import_data(self, start='last', end='now'):
        """ Download data from GDax for specific time interval
        
        Parameters
        ----------
        start : int
            Timestamp of the first observation of you want.
        end : int
            Timestamp of the last observation of you want.
        
        """
        self.start, self.end = self._set_time(start, end)
        param = {
            'start': TS_to_date(self.start - self.span),
            'end': TS_to_date(self.end),
            'granularity': self.span
        }
        r = requests.get(
            'https://api.gdax.com/products/{}/candles'.format(self.pair), 
            param
        )
        text = json.loads(r.text)
        data = [{
            'date': float(e[0]), 'open': float(e[3]), 'high': float(e[2]), 
            'low': float(e[1]), 'close': float(e[4]), 'volume': float(e[5]), 
            'quoteVolume': float(e[4]) * float(e[5])
        } for e in text]
        return data #self._sort_data(data)
    ImportDataCryptoCurrencies.import_data.__doc__ = _import_data.__doc__