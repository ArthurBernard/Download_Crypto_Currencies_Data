#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2019-08-07 11:16:51
# @Last modified by: ArthurBernard
# @Last modified time: 2019-08-10 10:56:39

""" Objects to download data from Bitmex exchange.

"""

# Built-in packages
import time
import asyncio
from datetime import datetime as dt

# Third party packages

# Local packages
from dccd.io_tools import IODataBase
from dccd.websocket_tools import DownloadDataWebSocket
from dccd.process_data import set_marketdepth, set_trades

__all__ = [
    'DownloadBitmexData', 'get_data_bitmex', 'get_orderbook_bitmex',
    'get_trades_bitmex',
]


# =========================================================================== #
#                              Parser functions                               #
# =========================================================================== #


def _parser_trades(tData, i=0):
    t = dt.strptime(tData['timestamp'], '%Y-%m-%dT%H:%M:%S.%f%z').timestamp()

    return {
        'tid': int(t * 1000 + i),
        'timestamp': int(t * 1000),
        'price': tData['price'],
        'amount': tData['size'],
        'type': tData['side'].lower(),
    }


def _parser_book(tData):
    if tData['side'] == 'Buy':
        s = 1
    else:
        s = -1

    if 'price' in tData.keys():

        return {'amount': s * tData['size'], 'price': tData['price']}

    return s * tData['size']


# =========================================================================== #
#                              Download objects                               #
# =========================================================================== #


class DownloadBitmexData(DownloadDataWebSocket):
    def __init__(self, time_step=60, until=3600):
        # TODO : set until parser to convert date, time, etc
        if until > time.time():
            until -= int(time.time())

        DownloadDataWebSocket.__init__(self, 'bitmex', time_step=time_step,
                                       STOP=until)
        self._parser_data = {
            'orderBookL2_25': self.parser_book,
            'trade': self.parser_trades,
            # 'candles': None,
        }
        self.d = {}
        self.start = False

    def parser_book(self, data):
        """ Parse data of order book. """
        action = data['action']

        for d in data['data']:
            if action == 'partial':  # or action == 'insert':
                self.d[d['id']] = _parser_book(d)
                self.start = True

            elif not self.start:
                self.logger.info("Waiting data")

                continue

            elif action == 'delete':
                self.d.pop(d['id'])

            elif action == 'insert':
                self.d[d['id']] = _parser_book(d)

            elif action == 'update':
                self.d[d['id']]['amount'] = _parser_book(d)

            else:
                self.logger.error('Unknown action {}: {}'.format(action, data))

        self._data[self.t] = {v['price']: v['amount'] for v in self.d.values()}

    def parser_trades(self, data):
        """ Parse data of trades. """
        i, _data = 0, []
        for d in data['data']:
            _data += [_parser_trades(d, i)]
            i += 1

        if self.t in self._data.keys():
            self._data[self.t] += _data

        else:
            self._data[self.t] = _data

    async def on_message(self, data):
        """ Set data to order book. """
        if isinstance(data, dict):
            if 'action' not in data.keys():
                self.logger.info('No action: {}'.format(data))
            else:
                self.parser(data)

        else:
            self.logger.error('Not recognizing: {}'.format(data))

    def __call__(self, *args):
        """ Open a websocket connection. """
        self.parser = self.get_parser(args[0])

        self.logger.info('Try connect WS and set {} stream.'.format(args[0]))

        self.loop = asyncio.get_event_loop()
        self.loop.run_until_complete(asyncio.gather(
            self._connect(args=':'.join(args)),
            self._loop()
        ))

        return self


# =========================================================================== #
#                            High level functions                             #
# =========================================================================== #

# TODO : Finish docstring


def get_data_bitmex(process_func, *args, time_step=60, until=None,
                    path=None, save_method='dataframe', io_params={},
                    **kwargs):
    """ Download orderbook from Bitfinex exchange. """
    # Set database connector object
    if path is None:
        path = 'database/orders/{}'.format(pair)

    # Set saver object
    saver = IODataBase(path, method=save_method)

    # Set websocket downloader object
    downloader = DownloadBitmexData(time_step=time_step, until=until)
    downloader.set_process_data(process_func)
    downloader.set_saver(saver, **io_params)

    downloader(*args)


def get_orderbook_bitmex(*args, time_step=60, until=None, path=None,
                         save_method='dataframe', io_params={}):
    """ Download orderbook from Bitmex exchange. """
    get_data_bitmex(set_marketdepth, *args, time_step=time_step, until=until,
                    path=path, save_method=save_method, io_params=io_params)


def get_trades_bitmex(*args, time_step=60, until=None, path=None,
                      save_method='dataframe', io_params={}):
    """ Download trades tick by tick from Bitmex exchange. """
    get_data_bitmex(set_trades, *args, time_step=time_step, until=until,
                    path=path, save_method=save_method, io_params=io_params)


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

    pair = 'XBTUSD'
    time_step = 60
    until = 0
    path_ord = '/home/arthur/database/bitmex/orders/XBTUSD/'
    path_tra = '/home/arthur/database/bitmex/trades/XBTUSD/'
    save_method = 'dataframe'
    io_params = {'name': '2019'}

    def f(x):
        return x

    # get_data_bitmex('candles', f, symbol=pair, key='trade:1m:tBTCUSD',
    #                  time_step=time_step, until=until, path=path,
    #                  save_method=save_method, io_params=io_params)

    # get_orderbook_bitmex('orderBookL2_25', pair, time_step=time_step,
    #                     until=until, path=path_ord, save_method=save_method,
    #                     io_params=io_params)

    get_trades_bitmex('trade', pair, time_step=time_step, until=until,
                      path=path_tra, save_method=save_method,
                      io_params=io_params)
