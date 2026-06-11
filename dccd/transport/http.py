"""Async HTTP client with retry/backoff."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import httpx

if TYPE_CHECKING:
    from dccd.transport.ratelimit import RateLimiter

__all__ = ["AsyncHTTPClient", "HTTPError"]

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = 30.0
_DEFAULT_RETRIES = 5
_DEFAULT_BACKOFF_BASE = 2.0


class HTTPError(Exception):
    """Raised when an HTTP request fails after all retries."""

    def __init__(self, status: int, url: str, body: str = "") -> None:
        self.status = status
        self.url = url
        super().__init__(f"HTTP {status} from {url}: {body[:200]}")


class AsyncHTTPClient:
    """Thin wrapper around httpx.AsyncClient with retry/backoff.

    Parameters
    ----------
    max_retries : int
        Number of retry attempts on transient errors (5xx, network errors).
    backoff_base : float
        Exponential backoff base in seconds.
    timeout : float
        Request timeout in seconds.
    exchange : str, optional
        Exchange name used to key the *proactive* rate limiter. When set
        together with *limiter*, every :meth:`get` awaits a token before the
        request, smoothing bursts (e.g. "run all jobs") to the exchange's
        published rate. ``None`` (default) disables proactive throttling.
    limiter : RateLimiter, optional
        Shared per-exchange limiter (typically
        :func:`dccd.transport.ratelimit.shared_limiter`). Only consulted when
        *exchange* is also set.
    """

    def __init__(
        self,
        max_retries: int = _DEFAULT_RETRIES,
        backoff_base: float = _DEFAULT_BACKOFF_BASE,
        timeout: float = _DEFAULT_TIMEOUT,
        headers: dict[str, str] | None = None,
        exchange: str | None = None,
        limiter: "RateLimiter | None" = None,
    ) -> None:
        self._max_retries = max_retries
        self._backoff_base = backoff_base
        self._timeout = timeout
        self._headers = headers or {}
        self._exchange = exchange
        self._limiter = limiter
        self._client: httpx.AsyncClient | None = None
        # Adapters share one AsyncHTTPClient and wrap each call in
        # ``async with self._http``. With two concurrent operations on the same
        # exchange (e.g. "run all jobs", or a scheduled job overlapping a manual
        # one) the first to finish would otherwise close the shared httpx client
        # mid-flight for the other ("Cannot send a request, as the client has
        # been closed"). Reference-count the context so the client is created on
        # first entry and closed only when the last concurrent user exits. Safe
        # under asyncio: the counter is mutated without intervening awaits.
        self._depth = 0

    async def __aenter__(self) -> "AsyncHTTPClient":
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=self._timeout,
                headers=self._headers,
                follow_redirects=True,
            )
        self._depth += 1
        return self

    async def __aexit__(self, *args: Any) -> None:
        self._depth -= 1
        if self._depth <= 0 and self._client:
            self._depth = 0
            await self._client.aclose()
            self._client = None

    async def get(self, url: str, params: dict[str, Any] | None = None) -> Any:
        """Perform a GET request with retry/backoff. Returns parsed JSON."""
        import asyncio

        client = self._client
        if client is None:
            raise RuntimeError("AsyncHTTPClient must be used as async context manager")

        last_exc: Exception | None = None
        for attempt in range(self._max_retries):
            try:
                # Proactive throttle: wait for a token before each outbound
                # request so concurrent operations on the same exchange stay
                # under its published rate (the shared limiter is keyed by
                # exchange). Reactive 429 handling below remains as a backstop.
                if self._limiter is not None and self._exchange is not None:
                    await self._limiter.acquire(self._exchange)
                resp = await client.get(url, params=params)
                if resp.status_code == 429:
                    retry_after = float(resp.headers.get("Retry-After", self._backoff_base))
                    logger.warning("Rate-limited by %s, sleeping %.1fs", url, retry_after)
                    await asyncio.sleep(retry_after)
                    continue
                if resp.status_code >= 500:
                    wait = self._backoff_base ** attempt
                    logger.warning("HTTP %d from %s, retry in %.1fs", resp.status_code, url, wait)
                    await asyncio.sleep(wait)
                    last_exc = HTTPError(resp.status_code, url, resp.text)
                    continue
                if resp.status_code >= 400:
                    raise HTTPError(resp.status_code, url, resp.text)
                return resp.json()
            except (httpx.NetworkError, httpx.TimeoutException) as exc:
                wait = self._backoff_base ** attempt
                logger.warning("Network error %s (attempt %d), retry in %.1fs", exc, attempt + 1, wait)
                await asyncio.sleep(wait)
                last_exc = exc

        if last_exc:
            raise last_exc
        raise RuntimeError(f"GET {url} failed after {self._max_retries} retries")
