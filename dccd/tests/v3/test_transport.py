"""Transport-layer tests — AsyncHTTPClient concurrency safety."""

import asyncio

import pytest

from dccd.transport.http import AsyncHTTPClient


@pytest.mark.asyncio
async def test_concurrent_context_does_not_close_client_early():
    """Two overlapping ``async with`` users share one client (review fix).

    Adapters share one AsyncHTTPClient; concurrent operations on the same
    exchange must not have the first finisher close the client out from under
    the other ("Cannot send a request, as the client has been closed").
    """
    http = AsyncHTTPClient()
    seen: list[bool] = []

    async def use(hold: float) -> None:
        async with http:
            await asyncio.sleep(hold)
            # While still inside the context, the client must be alive even if
            # another (shorter) user has already exited.
            seen.append(http._client is not None)

    await asyncio.gather(use(0.01), use(0.05))

    assert seen == [True, True]
    # Once the last user exits, the client is closed.
    assert http._client is None
