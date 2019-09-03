#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2019-03-26 10:42:57
# @Last modified by: ArthurBernard
# @Last modified time: 2019-09-03 22:05:18

""" Objects to download historical data from Poloniex exchange.

.. currentmodule:: dccd.histo_dl.poloniex

.. autoclass:: FromPoloniex
   :members: import_data, save, get_data
   :show-inheritance:

"""

# Import built-in packages

# Import third-party packages
import requests
import json

# Import local packages
from dccd.histo_dl.exchange import ImportDataCryptoCurrencies

__all__ = ['FromPoloniex']


class FromPoloniex(ImportDataCryptoCurrencies):
    """ Class to import crypto-currencies data from the Poloniex exchange.

    Parameters
    ----------
    path : str
        The path where data will be save.
    crypto : str
        The abreviation of the crypto-currency.
    span : {int, 'weekly', 'daily', 'hourly'}
        - If str, periodicity of observation.
        - If int, number of the seconds between each observation, minimal span\
            is 300 seconds.
    fiat : str
        A fiat currency or a crypto-currency. Poloniex don't allow fiat
        currencies, but USD theter.
    form : {'xlsx', 'csv'}
        Your favorit format. Only 'xlsx' and 'csv' for the moment.

    See Also
    --------
    FromBinance, FromGDax, FromKraken

    Notes
    -----
    See Poloniex API documentation [1]_ for more details on parameters.

    References
    ----------
    .. [1] https://docs.poloniex.com/#introduction

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
        if fiat in ['EUR', 'USD']:
            print("Poloniex don't allow fiat currencies.",
                  "The equivalent of US dollar is Tether USD as USDT.")
            self.fiat = fiat = 'USDT'

        if crypto == 'XBT':
            crypto = 'BTC'

        ImportDataCryptoCurrencies.__init__(
            self, path, crypto, span, 'Poloniex', fiat, form
        )

        self.pair = self.fiat + '_' + crypto
        self.full_path = self.path + '/Poloniex/Data/Clean_Data/'
        self.full_path += str(self.per) + '/'
        self.full_path += str(self.crypto) + str(self.fiat)

    def _import_data(self, start='last', end='now'):
        self.start, self.end = self._set_time(start, end)

        param = {
            'command': 'returnChartData',
            'currencyPair': self.pair,
            'start': self.start,
            'end': self.end,
            'period': self.span
        }

        r = requests.get('https://poloniex.com/public', param)

        return json.loads(r.text)

    def import_data(self, start='last', end='now'):
        """ Download data from Poloniex for specific time interval.

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
