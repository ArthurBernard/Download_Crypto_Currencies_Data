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
   :members: import_data, save, get_data, import_trades, save_trades, import_orderbook, save_orderbook
   :show-inheritance:

"""

from __future__ import annotations

# Import built-in packages
from datetime import datetime, timezone
from typing import Any

# Import third party packages
from dccd.histo_dl.exchange import ImportDataCryptoCurrencies

# Import local packages

__all__ = ['FromCoinbase']


class FromCoinbase(ImportDataCryptoCurrencies):
    """ Class to import crypto-currencies data from the Coinbase exchange.

    Parameters
    ----------
    path : str
        Root directory for data files.
    crypto : str
        Crypto-currency symbol, e.g. ``'BTC'``.
    span : int or str
        Candle interval in seconds (minimum 60) or a label such as
        ``'hourly'`` or ``'1h'``.
    fiat : str, optional
        Quote currency. Default is ``'USD'``. The pair is formatted with a
        hyphen separator (e.g. ``'BTC'`` + ``'USD'`` → ``'BTC-USD'``).
    form : str, optional
        Legacy parameter — ignored. Storage is always Parquet via
        :class:`~dccd.storage.DataStore`.
    tz : str, optional
        Timezone for date parsing: ``'local'`` (default), ``'UTC'``, or any
        IANA timezone name.

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
        Directory managed by :class:`~dccd.storage.DataStore` —
        ``{path}/coinbase/ohlc/{pair}/{span}/``.

    Methods
    -------
    import_data
    save
    get_data
    import_trades
    save_trades
    import_orderbook
    save_orderbook

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

    def __init__(self, path, crypto, span, fiat='USD', form='xlsx', tz='local'):
        ImportDataCryptoCurrencies.__init__(
            self, path, crypto, span, 'Coinbase', fiat, form, tz=tz
        )
        self.pair = self.format_pair(crypto, fiat)

    # Coinbase Exchange caps each candles request at ~300 bars; larger
    # windows return HTTP 400, so we page through the range in chunks.
    _MAX_CANDLES = 300

    def _import_data(self, start: int | str = 'last', end: int | str = 'now') -> list[dict[str, Any]]:
        self.start, self.end = self._set_time(start, end)
        url = 'https://api.exchange.coinbase.com/products/{}/candles'.format(
            self.pair
        )
        window = self._MAX_CANDLES * self.span
        data: list[dict[str, Any]] = []
        current = self.start
        while current < self.end:
            chunk_end = min(current + window, self.end)
            param = {
                'start': datetime.fromtimestamp(current, tz=timezone.utc).isoformat(),
                'end':   datetime.fromtimestamp(chunk_end, tz=timezone.utc).isoformat(),
                'granularity': self.span,
            }
            text = self._fetch(url, param).json()
            if not isinstance(text, list):
                raise RuntimeError(f"Coinbase API error: {text!r:.200}")
            if text and not isinstance(text[0], (list, tuple)):
                raise RuntimeError(f"Coinbase unexpected response: {text[:3]!r:.200}")
            data.extend({
                'date': float(e[0]),
                'open': float(e[3]),
                'high': float(e[2]),
                'low': float(e[1]),
                'close': float(e[4]),
                'volume': float(e[5]),
                'quoteVolume': float(e[4]) * float(e[5]),
            } for e in text)
            current = chunk_end

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
        data : pl.DataFrame
            Data sorted and cleaned in a data frame.

        """
        data = self._import_data(start=start, end=end)

        return self._sort_data(data)
