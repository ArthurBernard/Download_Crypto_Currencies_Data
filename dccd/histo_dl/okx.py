#!/usr/bin/env python3
# coding: utf-8

""" Objects to download historical data from OKX exchange.

.. currentmodule:: dccd.histo_dl.okx

.. autoclass:: FromOKX
   :members: import_data, save, get_data
   :show-inheritance:

"""

from __future__ import annotations

# Import built-in packages
from typing import Any

# Import third-party packages
# Import local packages
from dccd.histo_dl.exchange import ImportDataCryptoCurrencies

__all__ = ['FromOKX']

_OKX_INTERVALS = {
    60: '1m', 300: '5m', 900: '15m', 1800: '30m',
    3600: '1H', 7200: '2H', 14400: '4H', 21600: '6H', 43200: '12H',
    86400: '1D', 604800: '1W',
}


def okx_interval(span):
    """ Return the OKX bar string for the given span in seconds.

    Parameters
    ----------
    span : int
        Interval in seconds.

    Returns
    -------
    str
        OKX bar identifier.

    Examples
    --------
    >>> okx_interval(3600)
    '1H'

    >>> okx_interval(86400)
    '1D'

    """
    interval = _OKX_INTERVALS.get(span)
    if interval is None:
        raise ValueError(f"Unsupported OKX interval: {span}s")
    return interval


class FromOKX(ImportDataCryptoCurrencies):
    """ Class to import crypto-currencies data from the OKX exchange.

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
    FromBinance, FromCoinbase, FromKraken, FromBybit

    Notes
    -----
    Uses the OKX v5 REST API [1]_.

    References
    ----------
    .. [1] https://www.okx.com/docs-v5/en/#rest-api-market-data-get-candlesticks

    Attributes
    ----------
    pair : str
        Instrument ID (e.g. 'BTC-USDT').
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
        """ Return the OKX pair symbol for *crypto* and *fiat*.

        Parameters
        ----------
        crypto, fiat : str
            Asset symbols (e.g. ``'BTC'``, ``'USDT'``).

        Returns
        -------
        str
            Dash-separated pair (e.g. ``'BTC-USDT'``).

        """
        return crypto + '-' + fiat

    def __init__(self, path, crypto, span, fiat='USDT', form='xlsx'):
        """ Initialize object. """
        ImportDataCryptoCurrencies.__init__(
            self, path, crypto, span, 'OKX', fiat, form
        )
        self.pair = self.format_pair(crypto, fiat)
        self.full_path = self.path + '/OKX/Data/Clean_Data/'
        self.full_path += self.per + '/' + self.crypto + self.fiat

    def _import_data(self, start: int | str = 'last', end: int | str = 'now') -> list[dict[str, Any]]:
        self.start, self.end = self._set_time(start, end)

        param = {
            'instId': self.pair,
            'bar': okx_interval(self.span),
            'before': self.start * 1000,
            'after': self.end * 1000,
            'limit': 300,
        }

        r = self._fetch('https://www.okx.com/api/v5/market/candles', param)
        text = r.json()['data']
        text.reverse()

        data = [{
            'date': float(e[0]) / 1000,
            'open': float(e[1]),
            'high': float(e[2]),
            'low': float(e[3]),
            'close': float(e[4]),
            'volume': float(e[5]),
            'quoteVolume': float(e[7]),
        } for e in text]

        return data

    def import_data(self, start: int | str = 'last', end: int | str = 'now') -> ImportDataCryptoCurrencies:
        """ Download data from OKX for a specific time interval.

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
