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
from dccd.domain.records import OHLCBar
from dccd.domain.timeutils import NS, align_ns

__all__ = ["paginate_forward", "paginate_backward", "paginate_ohlc", "paginate_trades"]

logger = logging.getLogger(__name__)

T = TypeVar("T")


async def paginate_forward(
    fetch_page: Callable[[int, int, int], Coroutine[Any, Any, list[T]]],
    start_ns: int,
    end_ns: int,
    window_s: int,
    max_per_request: int,
    *,
    align_s: int | None = None,
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
    align_s : int or None
        Granularity to snap *start_ns* to (e.g. the candle span). Defaults to
        ``window_s``; snapping to the *window* would pull the start back by up
        to a whole window (e.g. ~41 days for 1h candles), fetching data the
        caller never asked for. Snap to the bar instead so the requested start
        is honoured.
    emit_progress : callable or None
        Called with ``(windows_done, windows_total)`` after each page.
    """
    window_ns = window_s * NS
    snap = align_s if align_s is not None else window_s
    cur = align_ns(start_ns, snap) if snap >= 60 else start_ns
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
    # Window = span * max_per_request so each call fills exactly one page, but
    # snap the start to the bar (span) — not the window — so the requested start
    # is honoured rather than pulled back by up to one whole window.
    window_s = span * max_per
    async for bar in paginate_forward(
        fetch_page, start_ns, end_ns, window_s, max_per,
        align_s=span, emit_progress=emit_progress,
    ):
        yield bar


async def paginate_trades(
    fetch_page: Callable[
        [int, int, int, str | None],
        Coroutine[Any, Any, tuple[list[T], str | None]],
    ],
    cap: Capability,
    start_ns: int,
    end_ns: int,
    *,
    emit_progress: Callable[[int, int], None] | None = None,
    max_pages: int = 1_000_000,
) -> AsyncIterator[T]:
    """Paginate by **cursor**, draining the ``[start_ns, end_ns]`` window.

    Despite the name, this paginator is generic over any record type that is
    duck-typed on ``.ts`` (see :func:`_get_ts`) — it drives the TRADES branch
    of :func:`~dccd.application.operations.backfill` and is reused unchanged
    for FUNDING (both are cursor-paged, sparse-relative-to-OHLC record
    streams). Unlike OHLC (fixed-size time windows), trades are far denser
    than any single page: a one-day window on a liquid pair holds millions of
    trades but a page is capped at ``cap.max_per_request``. Advancing by a
    fixed time window — the previous design — silently dropped everything
    past the first page. This paginator instead follows the adapter's opaque
    cursor until the window is exhausted.

    Parameters
    ----------
    fetch_page : async callable
        Closure with ``symbol`` bound:
        ``fetch_page(start_ns, end_ns, limit, cursor) -> (items, next_cursor)``.
        ``cursor`` is ``None`` on the first call.
    cap : Capability
        Source capability (provides ``max_per_request``).
    start_ns, end_ns : int
        Inclusive time range in nanoseconds. Items outside it are filtered out.
    emit_progress : callable or None
        Called with ``(pages_done, -1)`` after each page (total is unknown).
    max_pages : int
        Hard safety cap on the number of pages, to bound a misbehaving cursor.
    """
    max_per = cap.max_per_request or 1000
    cursor: str | None = None
    pages = 0

    while pages < max_pages:
        items, next_cursor = await fetch_page(start_ns, end_ns, max_per, cursor)
        out_of_window = False
        for item in items:
            ts = _get_ts(item)
            if ts > end_ns:
                out_of_window = True
                break
            if ts >= start_ns:
                yield item
        pages += 1
        if emit_progress:
            emit_progress(pages, -1)
        if out_of_window or next_cursor is None or next_cursor == cursor:
            break
        cursor = next_cursor
