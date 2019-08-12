#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2019-03-25 19:31:56
# @Last modified by: ArthurBernard
# @Last modified time: 2019-08-12 16:57:17

""" Objects to download data from Bitfinex exchange.

"""

# Built-in packages
import time
import asyncio
import logging

# Third party packages

# Local packages
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
    if tData[1] == 'te':
            tData = tData[2]

    return {
        'tid': tData[0],
        'timestamp': tData[1],
        'price': tData[3],
        'amount': abs(tData[2]),
        'type': 'buy' if tData[2] > 0. else 'sell',
    }


def _parser_book(tData):
    if isinstance(tData[1], list):
        tData = tData[1]

    return {'price': str(tData[0]), 'count': tData[1], 'amount': tData[2]}


# =========================================================================== #
#                              Download objects                               #
# =========================================================================== #


class DownloadBitfinexData(DownloadDataWebSocket):
    """ Basis object to download data from a stream websocket client API.

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
    set_process_data(func, **kwargs)
        Set a function and parameters to process/clean data before to be saved.
    set_saver(call, **kwargs)
        Set a callable object and parameters to save data or update a database.
    __call__(channel, **kwargs)
        Run asynchronously two loops to get data from bitfinex websocket and
        save/update the database.

    TODO :
    - None time_step send tick by tick data
    - Clean private/public methods
    - Add optional setting parser

    """
    # TODO : docstring
    # TODO : add more parser methods

    def __init__(self, time_step=60, until=3600):
        """ Initialize object.

        Parameters
        ----------
        time_step : int, optional
            Number of seconds between two snapshots of data, minimum is 1,
            default is 60 (one minute). Each `time_step` data will be
            processed and updated to the database.
        until : int, optional
            Number of seconds before stoping or timestamp of when stoping,
            default is 3600 (one hour).

        """
        # TODO : set until parser to convert date, time, etc
        if until > time.time():
            until -= int(time.time())

        DownloadDataWebSocket.__init__(self, 'bitfinex', time_step=time_step,
                                       STOP=until)
        self._parser_data = {
            'book': self.parser_book,
            'book_raw': self.parser_raw_book,
            'trades': self.parser_trades,
            'trades_raw': self.parser_raw_trades,
            # 'candles': None,
        }
        self.logger = logging.getLogger(__name__)
        self.d = {}

    def parser_raw_book(self, data):
        """ Parse raw order book, each timestep set in a list all orders.

        Parameters
        ----------
        data : list
            Order data.

        """
        data = _parser_book(data)

        self._raw_parser(data)

    def parser_book(self, data):
        """ Parse market depth of order book.

        Parameters
        ----------
        data : list
            Order data.

        """
        data = _parser_book(data)

        if data['count'] > 0:
            if data['price'] in self.d.keys():
                self.d[data['price']]['amount'] += data['amount']

            else:
                self.d[data['price']] = data

        else:
            self.d.pop(data['price'])

        self._data[self.t] = {v['price']: v['amount'] for v in self.d.values()}

    def parser_raw_trades(self, data):
        """ Parse trade data.

        Parameters
        ----------
        data : list
            Trade data.

        """
        if data[1] == 'tu':

            return

        data = _parser_trades(data)

        self._raw_parser(data)

    def parser_trades(self, data):
        """ Parse OHLC data.

        Parameters
        ----------
        data : list
            Trade data.

        """
        # TODO : process ohlc
        if data[1] == 'tu':

            return

        data = _parser_trades(data)

        self._raw_parser(data)

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
        """ Open a websocket connection and save/update the database.

        Run asynchronously two loops to get data from bitfinex websocket and
        save/update the database.

        Parameters
        ----------
        channel : str {'book', 'book_raw', 'trades', 'trades_raw'}
            Channel to get data, by default data will be aggregated (OHLC for
            'trades' and reconstructed orderbook for 'book'), add '_raw' to the
            `channel` to get raw data (trade tick by tick or each orders).
        **kwargs
            Any revelevant keyword arguments will be passed to the websocket
            connector, see API documentation [1]_ for more details.

        Warnings
        --------
        'book_raw' and 'trades_raw' can be very memory expensive.

        References
        ----------
        .. [1] https://docs.bitfinex.com/v2/docs/ws-public

        """
        self.parser = self.get_parser(channel)

        channel = channel[:-4] if channel[:-4] == '_raw' else channel

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
        path = 'database/orders/{}'.format(pair)

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
