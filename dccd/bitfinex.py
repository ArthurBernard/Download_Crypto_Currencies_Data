#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2019-03-25 19:31:56
# @Last modified by: ArthurBernard
# @Last modified time: 2019-08-10 10:47:22

""" Objects to download data from Bitfinex exchange.

"""

# Built-in packages
import os
import pathlib
import time
import asyncio
import logging

# Third party packages
import requests
import json

# Local packages
# from dccd.time_tools import *
from dccd.exchange import ImportDataCryptoCurrencies
from dccd.io_tools import IODataBase
from dccd.websocket_tools import DownloadDataWebSocket
from dccd.process_data import set_marketdepth, set_trades

__all__ = [
    'FromBitfinex', 'DownloadBitfinexData', 'get_data_bitfinex',
    'get_orderbook_bitfinex', 'get_trades_bitfinex',
]

# TODO : - get_raw_orderbook; get_ohlc;


class FromBitfinex(ImportDataCryptoCurrencies):
    pass


# =========================================================================== #
#                              Parser functions                               #
# =========================================================================== #


def _parser_trades(tData):
    return {
        'tid': tData[0],
        'timestamp': tData[1],
        'price': tData[3],
        'amount': abs(tData[2]),
        'type': 'buy' if tData[2] > 0. else 'sell',
    }


def _parser_book(tData):
    return {'price': str(tData[0]), 'count': tData[1], 'amount': tData[2]}


# =========================================================================== #
#                              Download objects                               #
# =========================================================================== #


class DownloadBitfinexData(DownloadDataWebSocket):
    # TODO : docstring
    # TODO : add more parser methods
    def __init__(self, time_step=60, until=3600):
        # TODO : set until parser to convert date, time, etc
        if until > time.time():
            until -= int(time.time())

        DownloadDataWebSocket.__init__(self, 'bitfinex', time_step=time_step,
                                       STOP=until)
        self.logger = logging.getLogger(__name__)
        self._parser_data = {
            'book': self.parser_book,
            'trades': self.parser_trades,
            # 'candles': None,
        }
        self.d = {}

    def parser_book(self, data):
        """ Parse data of order book. """
        if isinstance(data[1], list):
            data = _parser_book(data[1])
        else:
            data = _parser_book(data)

        if data['count'] > 0:
            if data['price'] in self.d.keys():
                self.d[data['price']]['amount'] += data['amount']

            else:
                self.d[data['price']] = data

        else:
            self.d.pop(data['price'])

        self._data[self.t] = {v['price']: v['amount'] for v in self.d.values()}

    def parser_trades(self, data):
        """ Parse data of trades. """
        if data[1] == 'tu':

            return

        elif data[1] == 'te':
            data = _parser_trades(data[2])

        else:
            data = _parser_trades(data)

        if self.t in self._data.keys():
            self._data[self.t] += [data]

        else:
            self._data[self.t] = [data]

    async def on_message(self, data):
        """ Set data to order book. """
        if isinstance(data, list):
            if data[1] == 'hb':
                self.logger.info('HeartBeat')

            elif isinstance(data[1][0], list):

                for d in data[1]:
                    self.parser(d)

            else:
                self.parser(data)

        else:
            self.logger.info('{}'.format(data))

    def __call__(self, channel, **kwargs):
        """ Open a websocket connection. """
        self.parser = self.get_parser(channel)  # self.parse_data[channel]

        self.logger.info('Try connect WS and set {} stream.'.format(channel))

        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(asyncio.gather(
            self._connect(channel=channel, **kwargs),
            self._loop()
        ))

        return self


# =========================================================================== #
#                            High level functions                             #
# =========================================================================== #

# TODO : Finish docstring


def get_data_bitfinex(channel, process_func, time_step=60, until=None,
                      path=None, save_method='dataframe', io_params={},
                      **kwargs):
    """ Download orderbook from Bitfinex exchange. """
    # Set database connector object
    if path is None:
        path = f'database/orders/{pair}'

    # Set saver object
    saver = IODataBase(path, method=save_method)

    # Set websocket downloader object
    downloader = DownloadBitfinexData(time_step=time_step, until=until)
    downloader.set_process_data(process_func)
    downloader.set_saver(saver, **io_params)

    downloader(channel, **kwargs)


def get_orderbook_bitfinex(symbol, precision='P0', frequency='F0', lenght='25',
                           time_step=60, until=None, path=None,
                           save_method='dataframe', io_params={}):
    """ Download orderbook from Bitfinex exchange. """
    get_data_bitfinex('book', set_marketdepth, time_step=time_step,
                      until=until, path=path, save_method=save_method,
                      io_params=io_params, symbol=symbol, precision=precision,
                      frequency=frequency, lenght=lenght)


def get_trades_bitfinex(symbol, time_step=60, until=None, path=None,
                        save_method='dataframe', io_params={}):
    """ Download trades tick by tick from Bitfinex exchange. """
    get_data_bitfinex('trades', set_trades, time_step=time_step, until=until,
                      path=path, save_method=save_method, io_params=io_params,
                      symbol=symbol)


# =========================================================================== #
#                                   Tests                                     #
# =========================================================================== #

# TODO : Clean this part

if __name__ == '__main__':

    import yaml
    import logging.config

    logging_path = '/home/arthur/Data/bitfinex_data_bot/scripts/logging.ini'
    with open(logging_path, 'rb') as f:
        config = yaml.safe_load(f.read())

    logging.config.dictConfig(config)

    pair = 'tBTCUSD'
    time_step = 1
    until = 10
    path_ord = '/home/arthur/database/bitfinex/orders/tBTCUSD/'
    path_tra = '/home/arthur/database/bitfinex/trades/tBTCUSD/'
    save_method = 'dataframe'
    io_params = {'name': '2019'}

    def f(x):
        return x

    # get_data_bitfinex('candles', f, symbol=pair, key='trade:1m:tBTCUSD',
    #                  time_step=time_step, until=until, path=path,
    #                  save_method=save_method, io_params=io_params)

    get_orderbook_bitfinex(pair, time_step=time_step, until=until,
                           path=path_ord, save_method=save_method,
                           io_params=io_params)

    get_trades_bitfinex(pair, time_step=time_step, until=until,
                        path=path_tra, save_method=save_method,
                        io_params=io_params)
