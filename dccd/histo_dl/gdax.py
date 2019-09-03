#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2019-02-13 18:25:19
# @Last modified by: ArthurBernard
# @Last modified time: 2019-09-03 22:04:48

""" Objects to download historical data from GDax exchange.

.. currentmodule:: dccd.histo_dl.gdax

.. autoclass:: FromGDax
   :members: import_data, save, get_data
   :show-inheritance:

"""

# Import built-in packages

# Import third party packages
import requests
import json

# Import local packages
from dccd.tools.date_time import TS_to_date
from dccd.histo_dl.exchange import ImportDataCryptoCurrencies

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
        - If str, periodicity of observation.
        - If int, number of the seconds between each observation, minimal span\
            is 60 seconds.
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

    def __init__(self, path, crypto, span, fiat='USD', form='xlsx'):
        """ Initialize object. """
        if crypto is 'XBT':
            crypto = 'BTC'
        ImportDataCryptoCurrencies.__init__(
            self, path, crypto, span, 'GDAX', fiat, form
        )
        self.pair = crypto + '-' + fiat
        self.full_path = self.path + '/GDAX/Data/Clean_Data/'
        self.full_path += self.per + '/' + self.crypto + self.fiat

    def _import_data(self, start='last', end='now'):
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
            'date': float(e[0]),
            'open': float(e[3]),
            'high': float(e[2]),
            'low': float(e[1]),
            'close': float(e[4]),
            'volume': float(e[5]),
            'quoteVolume': float(e[4]) * float(e[5])
        } for e in text]

        return data

    def import_data(self, start='last', end='now'):
        """ Download data from GDax for specific time interval.

        Parameters
        ----------
        start : int or str
            Timestamp of the first observation of you want as int or date
            format 'yyyy-mm-dd hh:mm:ss' as string.
        end : int or str
            Timestamp of the last observation of you want as int or date
            format 'yyyy-mm-dd hh:mm:ss' as string.

        Returns
        -------
        data : pd.DataFrame
            Data sorted and cleaned in a data frame.

        """
        data = self._import_data(start=start, end=end)

        return self._sort_data(data)
