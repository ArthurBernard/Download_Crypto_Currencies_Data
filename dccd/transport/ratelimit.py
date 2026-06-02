"""Token-bucket rate limiter per exchange."""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from typing import AsyncIterator

__all__ = ["RateLimiter", "rate_limiter"]

_DEFAULT_RATES: dict[str, float] = {
    "binance": 10.0,
    "coinbase": 10.0,
    "kraken": 1.0,
    "bybit": 10.0,
    "okx": 10.0,
    "bitfinex": 10.0,
    "bitmex": 10.0,
}


class TokenBucket:
    """Single token-bucket for one exchange."""

    def __init__(self, rate: float) -> None:
        self._rate = rate
        self._tokens = rate
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last
            self._tokens = min(self._rate, self._tokens + elapsed * self._rate)
            self._last = now
            if self._tokens < 1.0:
                wait = (1.0 - self._tokens) / self._rate
                await asyncio.sleep(wait)
                self._tokens = 0.0
            else:
                self._tokens -= 1.0


class RateLimiter:
    """Global per-exchange rate limiter.

    Parameters
    ----------
    rates : dict
        Map of exchange name → requests per second.
    """

    def __init__(self, rates: dict[str, float] | None = None) -> None:
        self._rates = {**_DEFAULT_RATES, **(rates or {})}
        self._buckets: dict[str, TokenBucket] = {}

    def _bucket(self, exchange: str) -> TokenBucket:
        if exchange not in self._buckets:
            rate = self._rates.get(exchange, 5.0)
            self._buckets[exchange] = TokenBucket(rate)
        return self._buckets[exchange]

    async def acquire(self, exchange: str) -> None:
        await self._bucket(exchange).acquire()

    @asynccontextmanager
    async def __call__(self, exchange: str) -> AsyncIterator[None]:
        await self.acquire(exchange)
        yield


_global_limiter = RateLimiter()


def rate_limiter(exchange: str) -> RateLimiter:
    """Return the global rate limiter (convenience accessor)."""
    return _global_limiter
