#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2019-08-07 11:16:51
# @Last modified by: ArthurBernard
# @Last modified time: 2019-09-03 21:48:45

""" Objects and functions to download data from Bitmex exchange.

.. currentmodule:: dccd.continuous_dl.bitmex

These functions and objects allow you to continuously download data and update
your database.

High level API
--------------

.. autofunction:: get_data_bitmex
.. autofunction:: get_orderbook_bitmex
.. autofunction:: get_trades_bitmex

Low level API
-------------

.. autoclass:: dccd.continuous_dl.bitmex.DownloadBitmexData
   :members: set_process_data, set_saver
   :special-members: __call__
   :show-inheritance:

"""

# Built-in packages
import time
import asyncio
from datetime import datetime as dt

# Third party packages

# Local packages
from dccd.tools.io import IODataBase
from dccd.continuous_dl.exchange import ContinuousDownloader
from dccd.process_data import set_marketdepth, set_trades

__all__ = [
    'DownloadBitmexData', 'get_data_bitmex', 'get_orderbook_bitmex',
    'get_trades_bitmex',
]

# TODO : get_raw_orderbook; get_ohlc;

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


class DownloadBitmexData(ContinuousDownloader):
    """ Basis object to download data from a stream websocket client API.

    Parameters
    ----------
    time_step : int, optional
        Number of seconds between two snapshots of data, minimum is 1, default
        is 60 (one minute). Each `time_step` data will be processed and updated
        to the database.
    until : int, optional
        Number of seconds before stoping or timestamp of when stoping, default
        is 3600 (one hour).

    Attributes
    ----------
    host : str
        Adress of host to connect.
    conn_par : dict
        Parameters of websocket connection.
    ws : websockets.client.WebSocketClientProtocol
        Connection with the websocket client.
    is_connect : bool
        True if is connected, False otherwise.
    ts : int
        Number of second between two snapshots of data.
    t : int
        Current timestamp but rounded by `ts`.
    until : int
        Timestamp to stop to download data.

    Methods
    -------
    set_process_data
    set_saver
    __call__

    """

    # TODO :
    # - None time_step send tick by tick data
    # - Clean private/public methods
    # - Add optional setting parser
    # TODO : docstring
    # TODO : add more parser methods

    def __init__(self, time_step=60, until=3600):
        """ Initialize object. """
        # TODO : set until parser to convert date, time, etc
        if until > time.time():
            until -= int(time.time())

        ContinuousDownloader.__init__(self, 'bitmex', time_step=time_step,
                                      STOP=until)
        self._parser_data = {
            'orderBookL2_25': self.parser_book,
            'trade': self.parser_trades,
            # 'candles': None,
        }
        self.d = {}
        self.start = False

    def parser_book(self, data):
        """ Parse data of order book.

        Parameters
        ----------
        data : dict
            Order data from the ws API.

        """
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
        """ Parse data of trades.

        Parameters
        ----------
        data : dict
            Order data from the ws API.

        """
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
        """ Open a websocket connection and save/update the database.

        Run asynchronously two loops to get data from Bitmex websocket and
        save/update the database.

        Parameters
        ----------
        channel : {'orderBookL2_25', 'trade'}
            Channel to get data, by default data will be aggregated (OHLC for
            'trades' and reconstructed orderbook for 'orderBookL2_25'), add
            '_raw' to the `channel` to get raw data (trade tick by tick or each
            orders).
        **kwargs
            Any revelevant keyword arguments will be passed to the websocket
            connector, see API documentation [1]_ for more details.

        Warnings
        --------
        '_raw' option not yet working for Bitmex.

        References
        ----------
        .. [1] https://www.bitmex.com/api/

        """
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
# TODO : Verify integration
# TODO : Verify docstring match with function signature

def get_data_bitmex(process_func, *args, time_step=60, until=None,
                    path=None, save_method='dataframe', io_params={},
                    **kwargs):
    """ Download data from Bitmex exchange and update the database.

    Parameters
    ----------
    channel : str, {'book', 'book_raw', 'trades', 'trades_raw'}
        Websocket channel to get data, by default data will be aggregated (OHLC
        for 'trades' and reconstructed orderbook for 'book'), add '_raw' to the
        `channel` to get raw data (trade tick by tick or each orders).
    process_func : callable
        Function to process and clean data before to be saved. Must take `data`
        in arguments and can take any optional keywords arguments, cf function
        exemples in :mod:`dccd.process_data`.
    process_params : dict, optional
        Dictionary of the keyword arguments available to `process_func`, cf
        documentation into :mod:`dccd.process_data`.
    save_method : {'DataFrame', 'SQLite', 'CSV', 'Excel', 'PostgreSQL',\
                   'Oracle', 'MSSQL', 'MySQL'},
        It will create an IODataBase object to save/update the database in the
        specified format `save_method`, default is 'DataFrame' it save as
        binary pd.DataFrame object. More informations are available into
        :mod:`dccd.io_tools`.
    io_params : dict, optional
        Dictionary of the keyword arguments available to the callable
        io_tools.IODataBase method. Note: With SQL format some parameters are
        compulsory, seed details into :mod:`dccd.io_tools`.
    time_step : int, optional
        Number of second between two snapshots of data, default 60 (1 minute).
    until : int, optional
        Number of seconds before stoping to download and update, default is
        None. If `until` equal 0 or None it means it never stop.
    path : str, optional
        Path to save/update the database, default is None. If `path` is None,
        database is saved at the relative path './database/bitmex/`channel`'.
    **kwargs
        Any revelevant keyword arguments will be passed to the websocket
        connector, see Bitmex API documentation [2]_ for more details.

    Warnings
    --------
    '_raw' option not yet working for bitmex.

    See Also
    --------
    process_data : function to process/clean data (set_marketdepth, set_ohlc,
        set_orders, set_marketdepth).
    io_tools.IODataBase : object to save/update the database with respect to
        specified format.

    References
    ----------
    .. [2] https://www.bitmex.com/api/

    """
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
