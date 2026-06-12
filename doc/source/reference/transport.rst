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

Rate limiting is **proactive**: a process-wide limiter keyed by exchange is
awaited before every outbound REST request, so concurrent operations on the
same exchange (e.g. "Run all" over many jobs) share one bucket and stay under
the exchange's published rate. Conservative per-exchange defaults are built in
(e.g. Kraken 1 req/s, Coinbase 3 req/s); reactive ``429``/``Retry-After``
handling in the HTTP client remains as a backstop. The HTTP connection pool is
held open for the whole paginated operation — one TLS session per backfill,
not one per page.

.. autoclass:: dccd.transport.ratelimit.RateLimiter
   :members:

.. autofunction:: dccd.transport.ratelimit.shared_limiter
