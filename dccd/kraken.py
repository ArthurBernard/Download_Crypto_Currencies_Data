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
from dccd.time_tools import *
from dccd.exchange import ImportDataCryptoCurrencies

__all__ = ['FromKraken']

class FromKraken(ImportDataCryptoCurrencies):
    """ Class to import crypto-currencies data from the Kraken exchange.
    
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
    FromBinance, FromGDax, FromPoloniex

    Notes
    -----
    See Kraken API documentation [1]_ for more details on parameters.

    References
    ----------
    .. [1] https://www.kraken.com/features/api
    
    """
    def __init__(self, path, crypto, span, fiat='USD', form='xlsx'):
        ImportDataCryptoCurrencies.__init__(
            self, path, crypto, span, 'Kraken', fiat=fiat, form=form
        )
        if crypto == 'BTC':
            crypto = 'XBT'
        if crypto == 'BCH' or crypto == 'DASH':
            self.pair = crypto+fiat
        elif fiat not in ['EUR', 'USD', 'CAD', 'JPY', 'GBP']:
            self.pair = 'X'+crypto+'X'+fiat
        else:
            self.pair = 'X'+crypto+'Z'+fiat
        
    
    def _import_data(self, start='last', end=None):
        """ Download data from Kraken since a specific time until now.
        
        Parameters
        ----------
        start : int or str
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
        return data #self._sort_data(data)
    ImportDataCryptoCurrencies.import_data.__doc__ = _import_data.__doc__