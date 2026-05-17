#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2026-05-12
# @Last modified by: ArthurBernard
# @Last modified time: 2026-05-12

""" Objects to download historical data from Coinbase exchange.

.. currentmodule:: dccd.histo_dl.coinbase

.. autoclass:: FromCoinbase
   :members: import_data, save, get_data
   :show-inheritance:

"""

from __future__ import annotations

# Import built-in packages
from datetime import datetime, timezone
from typing import Any

# Import third party packages
from dccd.histo_dl.exchange import ImportDataCryptoCurrencies

# Import local packages
from dccd.tools.date_time import TS_to_date

__all__ = ['FromCoinbase']


class FromCoinbase(ImportDataCryptoCurrencies):
    """ Class to import crypto-currencies data from the Coinbase exchange.

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
    FromBinance, FromKraken, FromBybit, FromOKX

    Notes
    -----
    See Coinbase Exchange API documentation [1]_ for more details on
    parameters. This class uses the public market data endpoint which does not
    require authentication.

    References
    ----------
    .. [1] https://docs.cdp.coinbase.com/exchange/reference/exchangerestapi_getproductcandles

    Attributes
    ----------
    pair : str
        Pair symbol, `crypto-fiat` (e.g. 'BTC-USD').
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

    @staticmethod
    def format_pair(crypto: str, fiat: str) -> str:
        """ Return the Coinbase pair symbol for *crypto* and *fiat*.

        Parameters
        ----------
        crypto, fiat : str
            Asset symbols (e.g. ``'BTC'``, ``'USD'``).

        Returns
        -------
        str
            Dash-separated pair (e.g. ``'BTC-USD'``).

        """
        if crypto == 'XBT':
            crypto = 'BTC'
        return crypto + '-' + fiat

    def __init__(self, path, crypto, span, fiat='USD', form='xlsx'):
        """ Initialize object. """
        ImportDataCryptoCurrencies.__init__(
            self, path, crypto, span, 'Coinbase', fiat, form
        )
        self.pair = self.format_pair(crypto, fiat)
        self.full_path = self.path + '/Coinbase/Data/Clean_Data/'
        self.full_path += self.per + '/' + self.crypto + self.fiat

    def _import_data(self, start: int | str = 'last', end: int | str = 'now') -> list[dict[str, Any]]:
        self.start, self.end = self._set_time(start, end)
        param = {
            'start': TS_to_date(self.start - self.span),
            'end': TS_to_date(self.end),
            'granularity': self.span,
        }
        r = self._fetch(
            'https://api.exchange.coinbase.com/products/{}/candles'.format(
                self.pair
            ),
            param,
        )
        text = r.json()
        data = [{
            'date': float(e[0]),
            'open': float(e[3]),
            'high': float(e[2]),
            'low': float(e[1]),
            'close': float(e[4]),
            'volume': float(e[5]),
            'quoteVolume': float(e[4]) * float(e[5]),
        } for e in text]

        return data

    def _import_trades(self, start: int, end: int) -> list[dict[str, Any]]:
        """ Fetch recent trades from Coinbase (recent data only).

        Notes
        -----
        The Coinbase Exchange public REST API returns up to 100 recent trades.
        Deep historical trades are not available without authenticated
        pagination.

        """
        r = self._fetch(
            f'https://api.exchange.coinbase.com/products/{self.pair}/trades',
            {'limit': 100},
        )
        result = []
        for e in r.json():
            ts = datetime.fromisoformat(
                e['time'].replace('Z', '+00:00')
            ).replace(tzinfo=timezone.utc).timestamp()
            result.append({
                'tid': int(e['trade_id']),
                'timestamp': float(ts),
                'price': float(e['price']),
                'amount': float(e['size']),
                'type': e['side'],
            })
        return result

    def _import_orderbook(self, depth: int = 50) -> list[dict[str, Any]]:
        r = self._fetch(
            f'https://api.exchange.coinbase.com/products/{self.pair}/book',
            {'level': 2},
        )
        book = r.json()
        result = []
        for bid in book['bids']:
            result.append({'side': 'bid', 'price': bid[0], 'amount': float(bid[1]), 'count': int(bid[2]) if len(bid) > 2 else None})
        for ask in book['asks']:
            result.append({'side': 'ask', 'price': ask[0], 'amount': float(ask[1]), 'count': int(ask[2]) if len(ask) > 2 else None})
        return result

    def import_data(self, start: int | str = 'last', end: int | str = 'now') -> ImportDataCryptoCurrencies:
        """ Download data from Coinbase for specific time interval.

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
