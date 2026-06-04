"""Async WebSocket base with auto-reconnect and checkpointing.

Exchange-specific WS inner classes only need to override :meth:`on_connect`
(send the subscription JSON) and either :meth:`parse_message` (yields domain
records from raw frames) or consume :meth:`stream_raw` directly when the
parse logic is too complex for a single async generator.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Any

__all__ = ["WebSocketBase"]

logger = logging.getLogger(__name__)

_INITIAL_DELAY = 1.0
_MAX_DELAY = 60.0
_BACKOFF_FACTOR = 2.0


class WebSocketBase:
    """Base async WebSocket client with exponential reconnect.

    Subclasses override :meth:`on_connect` to send subscription messages and
    :meth:`parse_message` to yield domain records from raw frames.

    For adapters that need a raw-frame async generator (e.g. to maintain
    local order-book state across messages), use :meth:`stream_raw` instead.

    Parameters
    ----------
    url : str
        WebSocket endpoint URL.
    """

    def __init__(self, url: str) -> None:
        self.url = url
        self._stop = asyncio.Event()

    def stop(self) -> None:
        """Request graceful shutdown."""
        self._stop.set()

    async def on_connect(self, ws: Any) -> None:
        """Called once after each (re)connect. Override to send subscriptions."""

    async def parse_message(self, raw: str | bytes) -> AsyncIterator[Any]:
        """Parse a raw frame and yield domain records. Override in subclass."""
        return
        yield  # make it an async generator

    async def stream(self) -> AsyncIterator[Any]:
        """Yield parsed domain records, reconnecting on errors.

        Delegates to :meth:`parse_message` for frame parsing.
        """
        async for raw in self.stream_raw():
            async for record in self.parse_message(raw):
                yield record

    async def stream_raw(self) -> AsyncIterator[str | bytes]:
        """Yield raw WebSocket frames, reconnecting with exponential backoff.

        Use this in adapters where :meth:`parse_message` is not convenient
        (e.g. stateful order-book reconstruction that spans multiple frames).
        """
        import websockets

        delay = _INITIAL_DELAY
        while not self._stop.is_set():
            try:
                # close_timeout keeps shutdown snappy: without it the closing
                # handshake blocks ~10s on stop/cancel (a "Stop" button that
                # appears frozen). 1s is plenty for a clean close.
                async with websockets.connect(self.url, close_timeout=1) as ws:
                    delay = _INITIAL_DELAY
                    await self.on_connect(ws)
                    async for raw in ws:
                        if self._stop.is_set():
                            return
                        yield raw
            except asyncio.CancelledError:
                return
            except Exception as exc:
                if self._stop.is_set():
                    return
                logger.warning(
                    "WS %s disconnected: %s — reconnect in %.1fs", self.url, exc, delay
                )
                await asyncio.sleep(delay)
                delay = min(delay * _BACKOFF_FACTOR, _MAX_DELAY)
