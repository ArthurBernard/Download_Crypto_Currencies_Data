"""Time utilities — nanosecond helpers, span mapping, interval labels.

All internal times in dccd v3 are **nanosecond int64 UTC**.
"""

from __future__ import annotations

import calendar
import datetime
import logging
import time

__all__ = [
    "NS",
    "ns_now",
    "s_to_ns",
    "ns_to_s",
    "ns_to_dt",
    "dt_to_ns",
    "str_to_ns",
    "align_ns",
    "str_to_span",
    "span_label",
    "binance_interval",
    "bybit_interval",
    "okx_interval",
    "kraken_interval",
    "coinbase_granularity",
]

_logger = logging.getLogger(__name__)

NS = 1_000_000_000

# ---------------------------------------------------------------------------
# Conversion helpers
# ---------------------------------------------------------------------------


def ns_now() -> int:
    """Return current time as nanoseconds UTC.

    Examples
    --------
    >>> ns_now() > 1_000_000_000_000_000_000
    True
    """
    return int(time.time() * NS)


def s_to_ns(ts_s: float) -> int:
    """Convert seconds timestamp to nanoseconds.

    Examples
    --------
    >>> s_to_ns(1548432099)
    1548432099000000000
    """
    return int(ts_s * NS)


def ns_to_s(ts_ns: int) -> float:
    """Convert nanoseconds timestamp to seconds.

    Examples
    --------
    >>> ns_to_s(1548432099000000000)
    1548432099.0
    """
    return ts_ns / NS


def ns_to_dt(ts_ns: int) -> datetime.datetime:
    """Convert nanoseconds timestamp to UTC datetime.

    Examples
    --------
    >>> ns_to_dt(1548432099000000000).year
    2019
    """
    return datetime.datetime.fromtimestamp(ts_ns / NS, tz=datetime.timezone.utc)


def dt_to_ns(dt: datetime.datetime) -> int:
    """Convert aware datetime to nanoseconds UTC.

    Examples
    --------
    >>> import datetime
    >>> dt = datetime.datetime(2019, 1, 25, 16, 1, 39, tzinfo=datetime.timezone.utc)
    >>> dt_to_ns(dt)
    1548432099000000000
    """
    return int(dt.timestamp() * NS)


def str_to_ns(date: str, form: str = "%Y-%m-%d %H:%M:%S", tz: str = "UTC") -> int:
    """Parse a date string and return nanoseconds UTC.

    Examples
    --------
    >>> str_to_ns('2019-01-25 16:01:39', tz='UTC')
    1548432099000000000
    """
    if form == "%Y-%m-%d %H:%M:%S" and len(date) == 10:
        form = "%Y-%m-%d"
    t = time.strptime(date, form)
    if tz.upper() == "LOCAL":
        ts_s = int(time.mktime(t))
    elif tz.upper() == "UTC":
        ts_s = int(calendar.timegm(t))
    else:
        from zoneinfo import ZoneInfo
        dt = datetime.datetime(*t[:6], tzinfo=ZoneInfo(tz))
        ts_s = int(dt.timestamp())
    return ts_s * NS


def align_ns(ts_ns: int, span_s: int) -> int:
    """Align *ts_ns* down to the nearest *span_s* boundary.

    Examples
    --------
    >>> align_ns(3700 * 1_000_000_000, 3600)
    3600000000000
    """
    span_ns = span_s * NS
    return (ts_ns // span_ns) * span_ns


# ---------------------------------------------------------------------------
# Span string helpers (kept from tools/date_time.py)
# ---------------------------------------------------------------------------

_STR_TO_SPAN: dict[str, int] = {
    # Monthly — use "1M" (capital M) to match exchange API conventions
    "monthly": 2592000, "month": 2592000, "1M": 2592000,
    "15d": 1296000,
    "weekly": 604800, "week": 604800, "7d": 604800, "1w": 604800, "w": 604800,
    "3d": 259200,
    "daily": 86400, "day": 86400, "24h": 86400, "1d": 86400, "d": 86400,
    "12h": 43200,
    "8h": 28800,
    "6h": 21600,
    "4h": 14400,
    "2h": 7200,
    "hourly": 3600, "hour": 3600, "1h": 3600, "h": 3600,
    "30m": 1800, "30min": 1800,
    "15m": 900, "15min": 900,
    "5m": 300, "5min": 300,
    "3m": 180, "3min": 180,
    # Minutes — lowercase m = minutes (exchange API convention: 1m, 3m, 5m…)
    "1m": 60, "minutely": 60, "minute": 60, "1min": 60, "min": 60,
    "60s": 60,
}

_SPAN_LABEL: dict[int, str] = {
    60: "1m", 180: "3m", 300: "5m", 900: "15m", 1800: "30m",
    3600: "1h", 7200: "2h", 14400: "4h", 21600: "6h", 28800: "8h",
    43200: "12h", 86400: "1d", 259200: "3d", 604800: "1w",
}


def str_to_span(string: str) -> int | None:
    """Return span in seconds for a human-readable string.

    Examples
    --------
    >>> str_to_span('1h')
    3600
    >>> str_to_span('daily')
    86400
    """
    result = _STR_TO_SPAN.get(string.lower())
    if result is None:
        _logger.warning("Unknown span string: %r", string)
    return result


def span_label(span: int) -> str:
    """Short filesystem-safe label for a span.

    Examples
    --------
    >>> span_label(3600)
    '1h'
    >>> span_label(7777)
    '7777s'
    """
    return _SPAN_LABEL.get(span, f"{span}s")


# ---------------------------------------------------------------------------
# Per-exchange interval formatters
# ---------------------------------------------------------------------------

def binance_interval(span: int) -> str | None:
    """Return Binance interval string for *span* seconds.

    Examples
    --------
    >>> binance_interval(3600)
    '1h'
    >>> binance_interval(86400)
    '1d'
    """
    if span % 60 == 0 and span // 60 in (1, 3, 5, 15, 30):
        return f"{span // 60}m"
    if span % 3600 == 0 and span // 3600 in (1, 2, 4, 6, 8, 12):
        return f"{span // 3600}h"
    if span == 86400:
        return "1d"
    if span == 259200:
        return "3d"
    if span == 604800:
        return "1w"
    if span == 2592000:
        return "1M"
    _logger.warning("No Binance interval for span=%d", span)
    return None


def bybit_interval(span: int) -> str | None:
    """Return Bybit interval string for *span* seconds.

    Examples
    --------
    >>> bybit_interval(3600)
    '60'
    >>> bybit_interval(86400)
    'D'
    """
    _map = {
        60: "1", 180: "3", 300: "5", 900: "15", 1800: "30",
        3600: "60", 7200: "120", 14400: "240", 21600: "360", 43200: "720",
        86400: "D", 604800: "W", 2592000: "M",
    }
    result = _map.get(span)
    if result is None:
        _logger.warning("No Bybit interval for span=%d", span)
    return result


def okx_interval(span: int) -> str | None:
    """Return OKX bar string for *span* seconds.

    Examples
    --------
    >>> okx_interval(3600)
    '1H'
    """
    _map = {
        60: "1m", 180: "3m", 300: "5m", 900: "15m", 1800: "30m",
        3600: "1H", 7200: "2H", 14400: "4H", 21600: "6H", 43200: "12H",
        86400: "1D", 604800: "1W", 2592000: "1M",
    }
    result = _map.get(span)
    if result is None:
        _logger.warning("No OKX interval for span=%d", span)
    return result


def kraken_interval(span: int) -> int | None:
    """Return Kraken interval in minutes for *span* seconds.

    Examples
    --------
    >>> kraken_interval(3600)
    60
    """
    if span % 60 != 0:
        _logger.warning("Kraken requires spans in whole minutes, got %d", span)
        return None
    minutes = span // 60
    valid = {1, 5, 15, 30, 60, 240, 1440, 10080, 21600}
    if minutes not in valid:
        _logger.warning("Kraken does not support %d-minute interval", minutes)
        return None
    return minutes


def coinbase_granularity(span: int) -> int | None:
    """Return Coinbase granularity (seconds) for *span*.

    Coinbase only supports fixed granularities.

    Examples
    --------
    >>> coinbase_granularity(3600)
    3600
    >>> coinbase_granularity(7200) is None
    True
    """
    valid = {60, 300, 900, 3600, 21600, 86400}
    if span in valid:
        return span
    _logger.warning("Coinbase does not support %d-second granularity", span)
    return None
