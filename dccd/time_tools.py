#!/usr/bin/env python3
# coding: utf-8
# @Author: ArthurBernard
# @Email: arthur.bernard.92@gmail.com
# @Date: 2019-02-13 16:53:04
# @Last modified by: ArthurBernard
# @Last modified time: 2019-08-14 18:49:11

""" Tools to manage some time functions with respect to specific API exchanges.

"""

# Built-in packages
import time

# Third party packages

# Local packages

__all__ = [
    'TS_to_date', 'date_to_TS', 'str_to_span', 'span_to_str',
    'binance_interval',
]


def TS_to_date(TS, form='%Y-%m-%d %H:%M:%S', local=True):
    """ Convert timestamp to date in specified format.

    Parameters
    ----------
    TS : int
        A timestamp to convert.
    form : str (default '%Y-%m-%d %H:%M:%S')
        Time format.
    local : bool (default is True)
        Local time is used if true else return UTC time.

    Returns
    -------
    date : str
        Date as specified format.

    Examples
    --------
    >>> TS_to_date(1548432099, form='%y-%m-%d %H:%M:%S', local=False)
    '19-01-25 16:01:39'

    """
    if local:
        date = time.localtime(TS)
    else:
        date = time.gmtime(TS)
    return time.strftime(form, date)


def date_to_TS(date, form='%Y-%m-%d %H:%M:%S'):
    """ Use your local time-zone to convert date in specific format to
    timestamp.

    Parameters
    ----------
    date : str
        A date to convert.
    form : str (default '%Y-%m-%d %H:%M:%S')
        Time format.

    Returns
    -------
    TS : int
        Timestamp of specified date.

    Examples
    --------
    # >>> date_to_TS('19-01-25 16:01:39', form='%y-%m-%d %H:%M:%S')
    # 1548428499

    """
    return int(time.mktime(time.strptime(date, form)))


# def TS_to_YMD(TS):
#    a = time.strftime('%Y %m %d', time.localtime(int(TS))).split(' ')
#    return dt.datetime(int(a[0]), int(a[1]), int(a[2]))


def str_to_span(string):
    """ Return the equivalent interval time in seconds.

    Parameters
    ----------
    string : str
        Time periodicity

    Returns
    -------
    span : int
        Number of seconds in time interval.

    Examples
    --------
    >>> str_to_span('minutely')
    60

    """
    if string.lower() in ['weekly', 'week', '7d', '1w', 'w']:
        return 604800
    elif string.lower() in ['daily', 'day', '24h', '1d', 'd']:
        return 86400
    elif string.lower() in ['bi-hourly', 'bi-hour', '2h']:
        return 7200
    elif string.lower() in ['hourly', 'hour', '1h', '60min', 'h']:
        return 3600
    elif string.lower() in ['half-hourly', 'half-hour', '30min']:
        return 1800
    elif string.lower() in ['5-minute', 'five-minute', '5 minute',
                            'five minute', '5min']:
        return 300
    elif string.lower() in ['minutely', 'minute', '1min', 'min']:
        return 60
    else:
        print('Error, string not understood.\nString must be "minutely",',
              '"5 minute", "hourly", "daily" or "weekly".')


def span_to_str(span):
    """ Return the time periodicity.

    Parameters
    ----------
    span : int
        Time interval in second.

    Returns
    -------
    date : str
        Time periodicity.

    Examples
    --------
    >>> span_to_str(3600)
    'Hourly'

    """
    if span == 60:
        return 'Minutely'
    elif span == 300:
        return 'Five_Minutely'
    elif span == 1800:
        return 'Half_Hourly'
    elif span == 3600:
        return 'Hourly'
    elif span == 7200:
        return 'Bi_Hourly'
    elif span == 86400:
        return 'Daily'
    elif span == 604800:
        return 'Weekly'
    else:
        print('Error, no string correspond to this time in seconds.')


def binance_interval(interval):
    """ Return the time interval in the specific format allowed by Binance.

    Parameters
    ----------
    interval : int
        Must be in seconds as 60, 180, 300, 900, 1800, 3600, 7200, 14400,
        21600, 28800, 43200, 86400, 259200, 604800, 2592000.

    Returns
    -------
    form : str
        Specific format allowed by Binance.

    Examples
    --------
    >>> binance_interval(7200)
    '2h'

    """
    if interval / 60 in [1, 3, 5, 15, 30]:
        return '{}m'.format(int(interval / 60))
    elif interval / 3600 in [1, 2, 4, 6, 8, 12]:
        return '{}h'.format(int(interval / 3600))
    elif interval / 86400 in [1, 3]:
        return '{}d'.format(int(interval / 86400))
    elif interval == 604800:
        return '1w'
    elif interval == 2592000:
        return '1M'
    else:
        print('No format allowed.')


if __name__ == '__main__':

    import doctest

    doctest.testmod()
