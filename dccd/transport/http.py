"""Async HTTP client with retry/backoff."""

from __future__ import annotations

import logging
from typing import Any

import httpx

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
    """

    def __init__(
        self,
        max_retries: int = _DEFAULT_RETRIES,
        backoff_base: float = _DEFAULT_BACKOFF_BASE,
        timeout: float = _DEFAULT_TIMEOUT,
        headers: dict[str, str] | None = None,
    ) -> None:
        self._max_retries = max_retries
        self._backoff_base = backoff_base
        self._timeout = timeout
        self._headers = headers or {}
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "AsyncHTTPClient":
        self._client = httpx.AsyncClient(
            timeout=self._timeout,
            headers=self._headers,
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self._client:
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
