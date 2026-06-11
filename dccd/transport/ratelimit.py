"""Token-bucket rate limiter, one bucket per exchange.

A single process-wide :class:`RateLimiter` (see :func:`shared_limiter`) is shared
by every adapter so that *concurrent* operations on the same exchange — e.g. a
"run all jobs" burst, or a scheduled backfill overlapping a manual one — draw
from one bucket and are serialised to the exchange's published rate, instead of
each adapter firing at the full rate independently. This is *proactive*
throttling; :class:`~dccd.transport.http.AsyncHTTPClient` keeps its *reactive*
429/Retry-After handling as a backstop.

Default rates are deliberately conservative (public, unauthenticated REST):

============  =========  ====================================================
exchange      req/s      source
============  =========  ====================================================
binance       10.0       weight-based, 1200 weight/min; klines weight 2 → high
coinbase       3.0       public endpoints 3 req/s (docs.cdp.coinbase.com)
kraken         1.0       public endpoints ~1 req/s (support.kraken.com)
bybit         10.0       public market data ~10 req/s
okx            8.0       history-candles 20 req/2s = 10/s; kept under for margin
bitfinex       1.0       public REST 10-90 req/min → ~1 req/s conservative
bitmex         0.5       unauthenticated ~30 req/min → 0.5 req/s
============  =========  ====================================================
"""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from typing import AsyncIterator, Awaitable, Callable

__all__ = ["RateLimiter", "shared_limiter"]

_DEFAULT_RATES: dict[str, float] = {
    "binance": 10.0,
    "coinbase": 3.0,
    "kraken": 1.0,
    "bybit": 10.0,
    "okx": 8.0,
    "bitfinex": 1.0,
    "bitmex": 0.5,
}

# Rate used for any exchange not listed in ``_DEFAULT_RATES``.
_FALLBACK_RATE = 3.0


class TokenBucket:
    """Single token-bucket for one exchange.

    Parameters
    ----------
    rate : float
        Sustained requests per second (also the burst capacity).
    clock : callable, optional
        Monotonic time source (seconds). Injectable for tests; defaults to
        :func:`time.monotonic`.
    sleep : callable, optional
        Async sleep coroutine factory. Injectable for tests; defaults to
        :func:`asyncio.sleep`.
    """

    def __init__(
        self,
        rate: float,
        *,
        clock: Callable[[], float] | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self._rate = rate
        self._tokens = rate
        self._clock = clock or time.monotonic
        self._sleep = sleep or asyncio.sleep
        self._last = self._clock()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        """Wait until a token is available, then consume it."""
        async with self._lock:
            now = self._clock()
            elapsed = now - self._last
            self._tokens = min(self._rate, self._tokens + elapsed * self._rate)
            self._last = now
            if self._tokens < 1.0:
                wait = (1.0 - self._tokens) / self._rate
                await self._sleep(wait)
                self._last = self._clock()
                self._tokens = 0.0
            else:
                self._tokens -= 1.0


class RateLimiter:
    """Per-exchange rate limiter holding one :class:`TokenBucket` each.

    Parameters
    ----------
    rates : dict, optional
        Map of exchange name → requests per second, merged over the conservative
        defaults. Unknown exchanges fall back to ``_FALLBACK_RATE``.
    clock : callable, optional
        Monotonic time source forwarded to each bucket (test seam).
    sleep : callable, optional
        Async sleep forwarded to each bucket (test seam).
    """

    def __init__(
        self,
        rates: dict[str, float] | None = None,
        *,
        clock: Callable[[], float] | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self._rates = {**_DEFAULT_RATES, **(rates or {})}
        self._clock = clock
        self._sleep = sleep
        self._buckets: dict[str, TokenBucket] = {}

    def _bucket(self, exchange: str) -> TokenBucket:
        if exchange not in self._buckets:
            rate = self._rates.get(exchange, _FALLBACK_RATE)
            self._buckets[exchange] = TokenBucket(
                rate, clock=self._clock, sleep=self._sleep
            )
        return self._buckets[exchange]

    async def acquire(self, exchange: str) -> None:
        """Wait until a token is available for *exchange*, then consume it."""
        await self._bucket(exchange).acquire()

    @asynccontextmanager
    async def __call__(self, exchange: str) -> AsyncIterator[None]:
        await self.acquire(exchange)
        yield


_shared_limiter = RateLimiter()


def shared_limiter() -> RateLimiter:
    """Return the process-wide :class:`RateLimiter` shared by all adapters.

    One instance per process means concurrent operations on the same exchange
    share a bucket and are throttled together. Wired in by adapters' default
    HTTP client construction.

    Returns
    -------
    RateLimiter
    """
    return _shared_limiter
