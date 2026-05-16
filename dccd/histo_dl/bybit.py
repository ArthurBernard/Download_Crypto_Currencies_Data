#!/usr/bin/env python3
# coding: utf-8

""" Objects to download historical data from Bybit exchange.

.. currentmodule:: dccd.histo_dl.bybit

.. autoclass:: FromBybit
   :members: import_data, save, get_data
   :show-inheritance:

"""

from __future__ import annotations

# Import built-in packages
from typing import Any

# Import third-party packages
# Import local packages
from dccd.histo_dl.exchange import ImportDataCryptoCurrencies

__all__ = ['FromBybit']

_BYBIT_INTERVALS = {
    60: '1', 180: '3', 300: '5', 900: '15', 1800: '30',
    3600: '60', 7200: '120', 14400: '240', 21600: '360', 43200: '720',
    86400: 'D', 604800: 'W',
}


def bybit_interval(span):
    """ Return the Bybit interval string for the given span in seconds.

    Parameters
    ----------
    span : int
        Interval in seconds.

    Returns
    -------
    str
        Bybit interval identifier.

    Examples
    --------
    >>> bybit_interval(3600)
    '60'

    >>> bybit_interval(86400)
    'D'

    """
    interval = _BYBIT_INTERVALS.get(span)
    if interval is None:
        raise ValueError(f"Unsupported Bybit interval: {span}s")
    return interval


class FromBybit(ImportDataCryptoCurrencies):
    """ Class to import crypto-currencies data from the Bybit exchange.

    Parameters
    ----------
    path : str
        The path where data will be save.
    crypto : str
        The abbreviation of the crypto-currency (e.g. 'BTC').
    span : {int, 'weekly', 'daily', 'hourly'}
        - If str, periodicity of observation.
        - If int, number of seconds between each observation.
    fiat : str, optional
        Quote currency, default is 'USDT'.
    form : {'xlsx', 'csv'}, optional
        Output format, default is 'xlsx'.

    See Also
    --------
    FromBinance, FromCoinbase, FromKraken, FromOKX

    Notes
    -----
    Uses the Bybit v5 REST API [1]_.

    References
    ----------
    .. [1] https://bybit-exchange.github.io/docs/v5/market/kline

    Attributes
    ----------
    pair : str
        Pair symbol (e.g. 'BTCUSDT').
    start, end : int
        Timestamps bounding the download.
    span : int
        Seconds between observations.

    Methods
    -------
    import_data
    save
    get_data

    """

    @staticmethod
    def format_pair(crypto: str, fiat: str) -> str:
        """ Return the Bybit pair symbol for *crypto* and *fiat*.

        Parameters
        ----------
        crypto, fiat : str
            Asset symbols (e.g. ``'BTC'``, ``'USDT'``).

        Returns
        -------
        str
            Concatenated pair (e.g. ``'BTCUSDT'``).

        """
        return crypto + fiat

    def __init__(self, path, crypto, span, fiat='USDT', form='xlsx'):
        """ Initialize object. """
        ImportDataCryptoCurrencies.__init__(
            self, path, crypto, span, 'Bybit', fiat, form
        )
        self.pair = self.format_pair(crypto, fiat)
        self.full_path = self.path + '/Bybit/Data/Clean_Data/'
        self.full_path += self.per + '/' + self.crypto + self.fiat

    def _import_data(self, start: int | str = 'last', end: int | str = 'now') -> list[dict[str, Any]]:
        self.start, self.end = self._set_time(start, end)

        param = {
            'category': 'spot',
            'symbol': self.pair,
            'interval': bybit_interval(self.span),
            'start': self.start * 1000,
            'end': self.end * 1000,
            'limit': 200,
        }

        r = self._fetch('https://api.bybit.com/v5/market/kline', param)
        text = r.json()['result']['list']
        text.reverse()

        data = [{
            'date': float(e[0]) / 1000,
            'open': float(e[1]),
            'high': float(e[2]),
            'low': float(e[3]),
            'close': float(e[4]),
            'volume': float(e[5]),
            'quoteVolume': float(e[6]),
        } for e in text]

        return data

    def import_data(self, start: int | str = 'last', end: int | str = 'now') -> ImportDataCryptoCurrencies:
        """ Download data from Bybit for a specific time interval.

        Parameters
        ----------
        start : int or str
            Timestamp of the first observation or date 'yyyy-mm-dd hh:mm:ss'.
        end : int or str
            Timestamp of the last observation or date 'yyyy-mm-dd hh:mm:ss'.

        Returns
        -------
        data : pd.DataFrame
            OHLCV data sorted and cleaned.

        """
        data = self._import_data(start=start, end=end)
        return self._sort_data(data)
