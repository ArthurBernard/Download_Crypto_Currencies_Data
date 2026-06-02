"""Generic paginator — forward (start→end) and backward (cursor-based).

The Paginator drives a source's fetch_*_page methods with the correct window
size derived from the source's declared Capability. This eliminates per-exchange
chunking code (generalises the Coinbase-300 fix to all adapters).
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


async def paginate_forward(
    fetch_page: Callable[..., Coroutine[Any, Any, list[T]]],
    start_ns: int,
    end_ns: int,
    span: int,
    max_per_request: int,
    *,
    emit_progress: Callable[[int, int], None] | None = None,
) -> AsyncIterator[T]:
    """Paginate forward from *start_ns* to *end_ns* in fixed windows.

    Parameters
    ----------
    fetch_page : async callable
        Called as ``await fetch_page(window_start_ns, window_end_ns, max_per_request)``.
    start_ns, end_ns : int
        Time range in nanoseconds.
    span : int
        Bar/item duration in seconds (used to compute window size).
    max_per_request : int
        Items per page (determines window size = span * max_per_request * NS).
    emit_progress : callable or None
        Called with ``(windows_done, windows_total)`` after each page.
    """
    window_ns = span * max_per_request * NS
    cur = align_ns(start_ns, span)
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
    fetch_page: Callable[..., Coroutine[Any, Any, tuple[list[T], str | None]]],
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
        Called as ``await fetch_page(cursor, max_per_request)`` where *cursor*
        is ``None`` on first call. Returns ``(items, next_cursor)``.
    emit_progress : callable or None
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
    if hasattr(item, "ts"):
        return item.ts
    return 0


async def paginate_ohlc(
    fetch_page: Callable[..., Coroutine[Any, Any, list[OHLCBar]]],
    cap: Capability,
    start_ns: int,
    end_ns: int,
    span: int,
    *,
    emit_progress: Callable[[int, int], None] | None = None,
) -> AsyncIterator[OHLCBar]:
    """Paginate OHLC bars forward using declared capacity."""
    max_per = cap.max_per_request or 1000
    async for bar in paginate_forward(
        fetch_page, start_ns, end_ns, span, max_per, emit_progress=emit_progress
    ):
        yield bar


async def paginate_trades(
    fetch_page: Callable[..., Coroutine[Any, Any, list[Trade]]],
    cap: Capability,
    start_ns: int,
    end_ns: int,
    *,
    emit_progress: Callable[[int, int], None] | None = None,
) -> AsyncIterator[Trade]:
    """Paginate trades forward using declared capacity."""
    max_per = cap.max_per_request or 1000
    async for trade in paginate_forward(
        fetch_page, start_ns, end_ns, 1, max_per, emit_progress=emit_progress
    ):
        yield trade
