"""Generic paginator — forward (start→end) and backward (cursor-based).

The Paginator drives a source's fetch_*_page methods. Adapters expose a fetch
function with signature ``fetch(start_ns, end_ns, limit) -> list[T]``; the
window size is derived from the source's declared Capability. This eliminates
per-exchange chunking code (generalises the Coinbase-300 fix to all adapters).

**Caller contract**: wrap the adapter's bound method in a closure that closes
over ``symbol`` (and ``span`` for OHLC) before passing it here. For example::

    async def _fetch(start_ns, end_ns, limit):
        return await adapter.fetch_ohlc_page(symbol, span, start_ns, end_ns, limit)
    async for bar in paginate_ohlc(_fetch, cap, start_ns, end_ns, span):
        ...
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Callable, Coroutine
from typing import Any, TypeVar

from dccd.domain.capability import Capability
from dccd.domain.records import OHLCBar, Trade
from dccd.domain.timeutils import NS, align_ns

__all__ = ["paginate_forward", "paginate_backward", "paginate_ohlc", "paginate_trades"]

logger = logging.getLogger(__name__)

T = TypeVar("T")

# Default time window for trades pagination (one calendar day at most).
_DEFAULT_TRADES_WINDOW_S = 86400


async def paginate_forward(
    fetch_page: Callable[[int, int, int], Coroutine[Any, Any, list[T]]],
    start_ns: int,
    end_ns: int,
    window_s: int,
    max_per_request: int,
    *,
    emit_progress: Callable[[int, int], None] | None = None,
) -> AsyncIterator[T]:
    """Paginate forward from *start_ns* to *end_ns* in fixed time windows.

    Parameters
    ----------
    fetch_page : async callable
        Signature: ``await fetch_page(window_start_ns, window_end_ns, max_per_request) -> list[T]``.
        Must already have ``symbol`` (and ``span`` for OHLC) bound via a closure.
    start_ns, end_ns : int
        Time range in nanoseconds.
    window_s : int
        Window duration in seconds. The paginator advances by ``window_s * NS``
        on each step regardless of how many items the page returned.
    max_per_request : int
        Passed as *limit* to each ``fetch_page`` call.
    emit_progress : callable or None
        Called with ``(windows_done, windows_total)`` after each page.
    """
    window_ns = window_s * NS
    cur = align_ns(start_ns, window_s) if window_s >= 60 else start_ns
    total_windows = max(1, (end_ns - cur + window_ns - 1) // window_ns)
    done = 0

    while cur < end_ns:
        chunk_end = min(cur + window_ns, end_ns)
        items = await fetch_page(cur, chunk_end, max_per_request)
        for item in items:
            yield item
        cur = chunk_end
        done += 1
        if emit_progress:
            emit_progress(done, total_windows)


async def paginate_backward(
    fetch_page: Callable[[str | None, int], Coroutine[Any, Any, tuple[list[T], str | None]]],
    start_ns: int,
    end_ns: int,
    max_per_request: int,
    *,
    emit_progress: Callable[[int, int], None] | None = None,
) -> AsyncIterator[T]:
    """Paginate backward using opaque cursors.

    Parameters
    ----------
    fetch_page : async callable
        Signature: ``await fetch_page(cursor, max_per_request) -> (items, next_cursor)``.
        *cursor* is ``None`` on the first call.
    """
    cursor: str | None = None
    page = 0

    while True:
        items, next_cursor = await fetch_page(cursor, max_per_request)
        filtered = [item for item in items if start_ns <= _get_ts(item) <= end_ns]
        for item in filtered:
            yield item
        page += 1
        if emit_progress:
            emit_progress(page, -1)
        if next_cursor is None:
            break
        if items and _get_ts(items[-1]) < start_ns:
            break
        cursor = next_cursor


def _get_ts(item: Any) -> int:
    return item.ts if hasattr(item, "ts") else 0


async def paginate_ohlc(
    fetch_page: Callable[[int, int, int], Coroutine[Any, Any, list[OHLCBar]]],
    cap: Capability,
    start_ns: int,
    end_ns: int,
    span: int,
    *,
    emit_progress: Callable[[int, int], None] | None = None,
) -> AsyncIterator[OHLCBar]:
    """Paginate OHLC bars forward using declared Capability.

    Parameters
    ----------
    fetch_page : async callable
        Must be a closure with ``symbol`` and ``span`` already bound:
        ``fetch_page(start_ns, end_ns, limit) -> list[OHLCBar]``.
    cap : Capability
        Source capability (provides ``max_per_request`` and ``page_direction``).
    start_ns, end_ns : int
        Time range in nanoseconds.
    span : int
        Candle interval in seconds — used to compute the page window.
    """
    max_per = cap.max_per_request or 1000
    # Window = span * max_per_request so each call fills exactly one page.
    window_s = span * max_per
    async for bar in paginate_forward(
        fetch_page, start_ns, end_ns, window_s, max_per, emit_progress=emit_progress
    ):
        yield bar


async def paginate_trades(
    fetch_page: Callable[[int, int, int], Coroutine[Any, Any, list[Trade]]],
    cap: Capability,
    start_ns: int,
    end_ns: int,
    *,
    window_s: int = _DEFAULT_TRADES_WINDOW_S,
    emit_progress: Callable[[int, int], None] | None = None,
) -> AsyncIterator[Trade]:
    """Paginate trades forward using declared Capability.

    Parameters
    ----------
    fetch_page : async callable
        Must be a closure with ``symbol`` already bound:
        ``fetch_page(start_ns, end_ns, limit) -> list[Trade]``.
    cap : Capability
        Source capability (provides ``max_per_request``).
    start_ns, end_ns : int
        Time range in nanoseconds.
    window_s : int
        Time-based window size in seconds per HTTP call (default 86 400 = 1 day).
        Tune this to keep each response near ``cap.max_per_request`` items.
    """
    max_per = cap.max_per_request or 1000
    async for trade in paginate_forward(
        fetch_page, start_ns, end_ns, window_s, max_per, emit_progress=emit_progress
    ):
        yield trade
