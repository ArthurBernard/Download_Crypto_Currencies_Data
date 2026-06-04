=========
Transport
=========

The async I/O building blocks every adapter shares: an HTTP client with
retry/backoff, a reconnecting WebSocket base, a token-bucket rate limiter, and
the generic paginators.

Pagination
==========

OHLC is paged by **fixed time windows** sized to one request
(``span × max_per_request``), snapped to the bar. Trades are paged by an
**opaque per-adapter cursor** that drains the whole window — this is what fixes
the capped-single-page data loss on liquid pairs. The application binds the
adapter's ``fetch_*_page`` in a closure and hands it to the paginator.

.. automodule:: dccd.transport.paginate
   :members:

HTTP client
===========

Shared by every adapter and reference-counted, so concurrent operations on the
same exchange don't close it out from under each other.

.. autoclass:: dccd.transport.http.AsyncHTTPClient
   :members:

.. autoexception:: dccd.transport.http.HTTPError

WebSocket base
==============

.. autoclass:: dccd.transport.ws.WebSocketBase
   :members:

Rate limiter
============

.. autoclass:: dccd.transport.ratelimit.RateLimiter
   :members:
