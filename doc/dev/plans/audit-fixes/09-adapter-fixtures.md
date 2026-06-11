---
plan: audit-fixes/09-adapter-fixtures
kind: leaf
status: planned
complexity: medium
depends: []
parallel: true
branch: chore/adapter-fixtures
pr: ""
---

# Tests — recorded payload fixtures for low-coverage adapters + transport units

## Goal

bitfinex (38 %), bitmex (39 %), coinbase (42 %), binance (50 %) parsing is
only exercised by the 3 opt-in network tests — exchange format drift or a
parsing regression ships silently. Record real REST/WS payloads as fixtures
and test the parse paths offline. Also cover the two near-untested transport
units: WS reconnect (49 %) and the token bucket (48 %).

## Files to change

- `dccd/tests/v3/fixtures/` (new) — JSON payload files, one per
  exchange × endpoint (`bitfinex_ohlc_page.json`,
  `bitfinex_ws_trade_msgs.json`, `bitmex_trades_page.json`,
  `coinbase_candles_page.json`, `binance_klines_page.json`, …). Captured
  from the live APIs **once, during this leaf**, with capture commands noted
  in a fixtures README (so they can be re-recorded when drift is suspected).
- `dccd/tests/v3/test_adapter_parsing.py` (new) — for each fixture: feed it
  through the adapter's page-parse / WS-message-parse function and assert
  record counts, ns timestamps, field mapping, ordering, and symbol
  normalization (XBT→BTC for bitmex/kraken paths).
- `dccd/tests/v3/test_transport.py` — add: `WebSocketBase.stream_raw()`
  reconnects after a dropped connection (fake ws server or monkeypatched
  `websockets.connect` yielding a failing then a working connection), with
  exponential backoff observed; `ratelimit.py` token bucket spaces two
  acquires (mock clock).

## Steps

1. Identify each adapter's parse seam (the function that turns a raw
   payload into records). If parsing is inlined into the fetch coroutine,
   extract a pure `_parse_*` helper — behavior-preserving, keeps domain
   purity rules.
2. Capture fixtures live (small pages, public endpoints, no keys) and
   commit them with the capture commands in the README.
3. Write the parsing tests; then the ws reconnect + ratelimit tests.
4. `pytest` (no `network` mark on any new test), `ruff`, `mypy`.

## Tests

This leaf *is* tests. Target: bitfinex/bitmex/coinbase ≥ 65 % each,
`transport/ws.py` and `ratelimit.py` ≥ 75 %.

## Verification on real data

- The fixture capture step itself is the real-data contact: each committed
  fixture must be a verbatim live response (note capture date + URL in the
  README). Cross-check one fixture per exchange against the corresponding
  `pytest -m network` test to prove parser ↔ live agreement on capture day.

## Closeout

- CHANGELOG (`Added`): "Offline adapter parsing tests from recorded live
  payloads; WS reconnect and rate-limiter unit tests (#NN)"
- ADR: none — mechanical.
- Status/roadmap: tick leaf; update coverage notes in `06-status.md`.
- NB: if leaf 05 chose to delete the RateLimiter, drop that part here.
