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
import calendar
import datetime
import logging
import time

# Third party packages

# Local packages

_logger = logging.getLogger(__name__)

__all__ = [
    'TS_to_date', 'date_to_TS', 'str_to_span', 'span_to_str',
    'span_label', 'binance_interval',
]

_SPAN_LABEL: dict[int, str] = {
    60: '1m', 180: '3m', 300: '5m', 900: '15m', 1800: '30m',
    3600: '1h', 7200: '2h', 14400: '4h', 21600: '6h', 28800: '8h',
    43200: '12h', 86400: '1d', 604800: '1w',
}


def TS_to_date(TS: int, form: str = '%Y-%m-%d %H:%M:%S', tz: str = 'local') -> str:
    """ Convert timestamp to date in specified format.

    Parameters
    ----------
    TS : int
        A timestamp to convert.
    form : str, optional
        strftime format string.  Default ``'%Y-%m-%d %H:%M:%S'``.
    tz : str, optional
        Timezone to use.  ``'local'`` (default) uses the system timezone,
        ``'UTC'`` uses UTC, any other value is interpreted as an IANA
        timezone name (e.g. ``'Europe/Paris'``).

    Returns
    -------
    str
        Date formatted according to *form*.

    Examples
    --------
    >>> TS_to_date(1548432099, form='%y-%m-%d %H:%M:%S', tz='UTC')
    '19-01-25 16:01:39'

    """
    if tz.upper() == 'LOCAL':
        return time.strftime(form, time.localtime(TS))
    elif tz.upper() == 'UTC':
        return time.strftime(form, time.gmtime(TS))
    else:
        from zoneinfo import ZoneInfo
        dt = datetime.datetime.fromtimestamp(TS, tz=ZoneInfo(tz))
        return dt.strftime(form)


def date_to_TS(date: str, form: str = '%Y-%m-%d %H:%M:%S', tz: str = 'local') -> int:
    """ Convert a date string to a Unix timestamp.

    Parameters
    ----------
    date : str
        Date string to convert.
    form : str, optional
        strftime format string.  Default ``'%Y-%m-%d %H:%M:%S'``.
    tz : str, optional
        Timezone used to interpret *date*.  ``'local'`` (default) uses the
        system timezone, ``'UTC'`` treats the string as UTC, any other value
        is an IANA timezone name (e.g. ``'Europe/Paris'``).

    Returns
    -------
    int
        Unix timestamp.

    Examples
    --------
    >>> date_to_TS('2019-01-25 16:01:39', tz='UTC')
    1548432099

    """
    if form == '%Y-%m-%d %H:%M:%S' and len(date) == 10:
        form = '%Y-%m-%d'
    t = time.strptime(date, form)
    if tz.upper() == 'LOCAL':
        return int(time.mktime(t))
    elif tz.upper() == 'UTC':
        return int(calendar.timegm(t))
    else:
        from zoneinfo import ZoneInfo
        dt = datetime.datetime(*t[:6], tzinfo=ZoneInfo(tz))
        return int(dt.timestamp())


# def TS_to_YMD(TS):
#    a = time.strftime('%Y %m %d', time.localtime(int(TS))).split(' ')
#    return dt.datetime(int(a[0]), int(a[1]), int(a[2]))


def str_to_span(string: str) -> int | None:
    """ Return the equivalent interval time in seconds.

    Parameters
    ----------
    string : str
        Time periodicity. Accepted values (case-insensitive):
        ``'monthly'``, ``'1M'``, ``'15d'``, ``'weekly'``, ``'3d'``,
        ``'daily'``, ``'12h'``, ``'8h'``, ``'6h'``, ``'4h'``,
        ``'bi-hourly'``, ``'hourly'``, ``'half-hourly'``, ``'quarter-hourly'``,
        ``'5min'``, ``'3m'``, ``'minutely'``, and common aliases.

    Returns
    -------
    span : int
        Number of seconds in time interval.

    Examples
    --------
    >>> str_to_span('minutely')
    60

    """
    s = string.lower()
    if s in ['monthly', 'month', '1m']:
        return 2592000
    elif s in ['15-daily', '15-day', '15d']:
        return 1296000
    elif s in ['weekly', 'week', '7d', '1w', 'w']:
        return 604800
    elif s in ['3-daily', '3-day', '3d']:
        return 259200
    elif s in ['daily', 'day', '24h', '1d', 'd']:
        return 86400
    elif s in ['12-hourly', '12-hour', '12h']:
        return 43200
    elif s in ['8-hourly', '8-hour', '8h']:
        return 28800
    elif s in ['6-hourly', '6-hour', '6h']:
        return 21600
    elif s in ['4-hourly', '4-hour', '4h']:
        return 14400
    elif s in ['bi-hourly', 'bi-hour', '2h']:
        return 7200
    elif s in ['hourly', 'hour', '1h', '60min', 'h']:
        return 3600
    elif s in ['half-hourly', 'half-hour', '30min']:
        return 1800
    elif s in ['quarter-hourly', 'quarter-hour', '15min', '15m']:
        return 900
    elif s in ['5-minute', 'five-minute', '5 minute', 'five minute', '5min']:
        return 300
    elif s in ['3-minute', 'three-minute', '3min', '3m']:
        return 180
    elif s in ['minutely', 'minute', '1min', 'min']:
        return 60
    else:
        _logger.warning(
            'Error, string not understood. Expected values such as "minutely", '
            '"5min", "15m", "hourly", "4h", "daily", "weekly", "monthly", etc.'
        )
        return None


def span_to_str(span: int) -> str | None:
    """ Return the time periodicity label for the given span in seconds.

    Parameters
    ----------
    span : int
        Time interval in seconds. Supported values: 60, 180, 300, 900, 1800,
        3600, 7200, 14400, 21600, 28800, 43200, 86400, 259200, 604800,
        1296000, 2592000.

    Returns
    -------
    date : str
        Time periodicity label used for directory naming.

    Examples
    --------
    >>> span_to_str(3600)
    'Hourly'

    """
    _map = {
        60: 'Minutely',
        180: 'Three_Minutely',
        300: 'Five_Minutely',
        900: 'Quarter_Hourly',
        1800: 'Half_Hourly',
        3600: 'Hourly',
        7200: 'Bi_Hourly',
        14400: 'Four_Hourly',
        21600: 'Six_Hourly',
        28800: 'Eight_Hourly',
        43200: 'Twelve_Hourly',
        86400: 'Daily',
        259200: 'Three_Daily',
        604800: 'Weekly',
        1296000: 'Fifteen_Daily',
        2592000: 'Monthly',
    }
    label = _map.get(span)
    if label is None:
        _logger.warning('Error, no string correspond to this time in seconds.')
    return label


def span_label(span: int) -> str:
    """ Return a short directory-safe label for *span* seconds.

    Parameters
    ----------
    span : int
        Candle interval in seconds.

    Returns
    -------
    str
        Short label (e.g. ``'1m'``, ``'1h'``, ``'1d'``).
        Falls back to ``'{span}s'`` for unknown spans.

    Examples
    --------
    >>> span_label(3600)
    '1h'
    >>> span_label(86400)
    '1d'
    >>> span_label(7777)
    '7777s'

    """
    return _SPAN_LABEL.get(span, f'{span}s')


def binance_interval(interval: int) -> str | None:
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
        _logger.warning('No format allowed.')
        return None


if __name__ == '__main__':

    import doctest

    doctest.testmod()
