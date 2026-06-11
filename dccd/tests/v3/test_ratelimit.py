"""Rate-limiter tests — token-bucket spacing and HTTP-client wiring.

These use injectable clock/sleep seams so they assert *pacing* deterministically
without real wall-clock waits.
"""

import asyncio

import httpx
import pytest

from dccd.transport.http import AsyncHTTPClient
from dccd.transport.ratelimit import RateLimiter, TokenBucket, shared_limiter


class FakeClock:
    """Manually-advanced monotonic clock with an async ``sleep`` that advances it.

    Captures every ``sleep`` duration so tests can assert the limiter waited the
    expected amount, without spending real time.
    """

    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def time(self) -> float:
        return self.now

    async def sleep(self, secs: float) -> None:
        self.sleeps.append(secs)
        self.now += secs


@pytest.mark.asyncio
async def test_two_rapid_acquires_space_out_per_rate():
    """A second immediate ``acquire`` waits ~1/rate when the bucket is drained.

    With rate=1/s and a fresh bucket (1 token), the first acquire is free; the
    second must sleep ~1s. With the fake clock no real time passes.
    """
    clock = FakeClock()
    limiter = RateLimiter({"kraken": 1.0}, clock=clock.time, sleep=clock.sleep)

    await limiter.acquire("kraken")  # consumes the single starting token
    assert clock.sleeps == []

    await limiter.acquire("kraken")  # bucket empty → must wait 1/rate
    assert clock.sleeps == [pytest.approx(1.0)]


@pytest.mark.asyncio
async def test_acquire_after_elapsed_time_is_free():
    """Tokens refill over time: waiting 1/rate before the 2nd acquire is free."""
    clock = FakeClock()
    limiter = RateLimiter({"kraken": 1.0}, clock=clock.time, sleep=clock.sleep)

    await limiter.acquire("kraken")
    clock.now += 1.0  # one token refills at rate=1/s
    await limiter.acquire("kraken")

    assert clock.sleeps == []  # no wait needed


@pytest.mark.asyncio
async def test_burst_capacity_equals_rate():
    """A fresh bucket allows a burst of ``rate`` acquires before throttling."""
    clock = FakeClock()
    bucket = TokenBucket(3.0, clock=clock.time, sleep=clock.sleep)

    for _ in range(3):
        await bucket.acquire()
    assert clock.sleeps == []  # 3 tokens available up front

    await bucket.acquire()  # 4th drains and must wait
    assert clock.sleeps == [pytest.approx(1.0 / 3.0)]


@pytest.mark.asyncio
async def test_per_exchange_buckets_are_independent():
    """Acquiring on one exchange does not throttle another."""
    clock = FakeClock()
    limiter = RateLimiter(
        {"kraken": 1.0, "binance": 1.0}, clock=clock.time, sleep=clock.sleep
    )

    await limiter.acquire("kraken")
    await limiter.acquire("binance")  # separate bucket → still has a token

    assert clock.sleeps == []


def test_shared_limiter_is_singleton():
    """``shared_limiter()`` returns the same process-wide instance."""
    assert shared_limiter() is shared_limiter()


@pytest.mark.asyncio
async def test_http_client_acquires_once_per_page(monkeypatch):
    """Each :meth:`AsyncHTTPClient.get` consumes exactly one limiter token.

    Integration-style: a fake transport returns a paged JSON payload; we count
    limiter acquisitions across N gets and assert one-per-get with the exchange
    key threaded through.
    """
    acquired: list[str] = []

    class CountingLimiter:
        async def acquire(self, exchange: str) -> None:
            acquired.append(exchange)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True})

    http = AsyncHTTPClient(exchange="okx", limiter=CountingLimiter())  # type: ignore[arg-type]
    transport = httpx.MockTransport(handler)

    async with http:
        # Swap the underlying httpx client for one backed by the mock transport.
        await http._client.aclose()  # type: ignore[union-attr]
        http._client = httpx.AsyncClient(transport=transport)
        for _ in range(5):
            await http.get("https://example.test/page")

    assert acquired == ["okx"] * 5


@pytest.mark.asyncio
async def test_http_client_without_limiter_does_not_throttle():
    """No exchange/limiter wired → ``get`` issues requests with no token wait."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True})

    http = AsyncHTTPClient()  # no exchange, no limiter
    transport = httpx.MockTransport(handler)

    async with http:
        await http._client.aclose()  # type: ignore[union-attr]
        http._client = httpx.AsyncClient(transport=transport)
        out = await http.get("https://example.test/x")

    assert out == {"ok": True}


@pytest.mark.asyncio
async def test_concurrent_acquires_on_same_exchange_serialize():
    """Concurrent acquires on one exchange draw from a single shared bucket.

    Three coroutines racing on rate=1/s must collectively wait so they are not
    all let through at once — proving "run all jobs" bursts are serialised.
    """
    clock = FakeClock()
    limiter = RateLimiter({"okx": 1.0}, clock=clock.time, sleep=clock.sleep)

    await asyncio.gather(*(limiter.acquire("okx") for _ in range(3)))

    # 1 free token, then 2 waits of ~1/rate each.
    assert len(clock.sleeps) == 2
    assert all(s == pytest.approx(1.0) for s in clock.sleeps)
