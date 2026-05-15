#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2019-02-13 18:25:01
# @Last modified by: ArthurBernard
# @Last modified time: 2019-09-03 22:05:04

""" Objects to download historical data from Kraken exchange.

.. currentmodule:: dccd.histo_dl.kraken

.. autoclass:: FromKraken
   :members: import_data, save, get_data
   :show-inheritance:

"""

from __future__ import annotations

# Import built-in packages
import time
import warnings
from typing import Any

# Import third party packages
# Import local packages
from dccd.histo_dl.exchange import ImportDataCryptoCurrencies

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
        - If str, periodicity of observation.
        - If int, number of the seconds between each observation, minimal span\
            is 60 seconds.
    fiat : str
        A fiat currency or a crypto-currency.
    form : {'xlsx', 'csv'}
        Your favorit format. Only 'xlsx' and 'csv' for the moment.

    See Also
    --------
    FromBinance, FromCoinbase, FromBybit, FromOKX

    Notes
    -----
    See Kraken API documentation [1]_ for more details on parameters.

    References
    ----------
    .. [1] https://www.kraken.com/features/api

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
        ImportDataCryptoCurrencies.__init__(
            self, path, crypto, span, 'Kraken', fiat=fiat, form=form
        )
        if crypto == 'BTC':
            crypto = 'XBT'

        if crypto == 'BCH' or crypto == 'DASH':
            self.pair = crypto + fiat

        elif fiat not in ['EUR', 'USD', 'CAD', 'JPY', 'GBP']:
            self.pair = 'X' + crypto + 'X' + fiat

        else:
            self.pair = 'X' + crypto + 'Z' + fiat

    def _import_data(
        self, start: int | str = 'last', end: int | str | None = None
    ) -> list[dict[str, Any]]:
        if end is not None:
            warnings.warn(
                "The Kraken OHLC API does not support an end date — the 'end' "
                "parameter is ignored and data is always fetched up to now.",
                UserWarning,
                stacklevel=2,
            )
        self.start, self.end = self._set_time(start, int(time.time()))

        param = {
            'pair': self.pair,
            'interval': int(self.span / 60),
            'since': self.start - self.span
        }

        r = self._fetch('https://api.kraken.com/0/public/OHLC', param)
        text = r.json()['result'][self.pair]

        data = [{
            'date': float(e[0]),
            'open': float(e[1]),
            'high': float(e[2]),
            'low': float(e[3]),
            'close': float(e[4]),
            'weightedAverage': float(e[5]),
            'volume': float(e[6]),
            'quoteVolume': float(e[6]) * float(e[5])
        } for e in text]

        return data

    def import_data(
        self, start: int | str = 'last', end: int | str | None = None
    ) -> ImportDataCryptoCurrencies:
        """ Download data from Kraken since a specific time until now.

        Parameters
        ----------
        start : int or str
            Timestamp of the first observation as a Unix timestamp (int) or a
            date string ``'yyyy-mm-dd hh:mm:ss'``.
        end : int, str or None
            Ignored. The Kraken OHLC API does not support a custom end date
            and always returns data up to the current time. Passing a non-None
            value raises a :class:`UserWarning`.

        Returns
        -------
        self : FromKraken
            Data sorted and cleaned in a data frame, accessible via
            :meth:`get_data`.

        """
        data = self._import_data(start=start, end=end)

        return self._sort_data(data)
