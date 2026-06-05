# 4 — Exchanges & capabilities

Seven spot adapters in `dccd/sources/`. Each declares `capabilities()`; the engine
honours them. This page is the factual matrix plus the caveats that actually shape
the code — the things that bite if you forget them.

## Capability summary

| Exchange | OHLC history | Trades history | Order-book history | OHLC live | Trades live | Book live |
|----------|--------------|----------------|--------------------|-----------|-------------|-----------|
| binance  | ✅ REST (1000/req) | ✅ REST cursor (`fromId`) | ❌ → WS | ✅ | ✅ | ✅ |
| bybit    | ✅ REST (1000) | ❌ recent-only → WS | ❌ → WS | ✅ | ✅ | ✅ |
| coinbase | ✅ REST (300/req) | ⚠️ slow cursor | ❌ → WS | ❌ (not impl.) | ✅ | ❌ (not impl.) |
| kraken   | ⚠️ **720 recent only** | ✅ REST full (`since`) | ❌ → WS | ✅ | ✅ | ✅ |
| okx      | ✅ `history-candles` | ⚠️ `history-trades` | ❌ → WS | ✅ | ✅ | ✅ |
| bitfinex | ✅ REST (10000) | ✅ REST (10000) | ❌ → WS | ✅ | ✅ | ❌ (not impl.) |
| bitmex   | ✅ bucketed (1m/5m/1h/1d only) | ✅ REST full | ❌ → WS | ✅ | ✅ | ✅ |

`❌ → WS` = no free historical order book anywhere; the only way to build history
is to record the WS stream over time, then read it back from the store.
Capabilities that aren't implemented are **not declared** (so the engine raises
`NoCapability` early rather than "running" empty).

## Caveats that drive the code

- **Kraken OHLC is recent-only (720 bars).** Deep history over REST is
  impossible; the engine clamps the start to the available window and warns. True
  deep Kraken OHLC must be **derived from trades** (`domain/transforms.py`
  exists for this; full wiring is deferred — see `06-status.md`).
- **Bybit spot trades have no deep history** (recent ~60). The adapter does *not*
  declare trades history → `NoCapability`. Bybit trades history is only built by
  forward-collecting the WS; OHLC backfill uses `kline` (which does have history).
- **BitMEX OHLC is bucketed**: only 1m/5m/1h/1d. The `spans` capability lists
  exactly those; other spans must be derived.
- **Pagination direction & caps differ** (60 → 10 000 per request; forward vs
  backward cursors). This is exactly why trades pagination is cursor-based and
  parameterised by capability, never hard-chunked per exchange.
- **Symbol formats vary**: binance/bybit `BTCUSDT`, coinbase/okx `BTC-USD(T)`,
  kraken `XBTUSD` (alias `XBT=BTC`, WS v2 uses `BTC/USD`), bitfinex `tBTCUSD`,
  bitmex `XBTUSD`. Hence the central `Symbol` with per-adapter render/parse.

## Order-book WS — best bid/ask correctness (2026-06 fix)

Live order books are the sharp edge: most exchanges stream **diff/delta** updates,
not a sorted snapshot, so naively taking `bids[0]`/`asks[0]` yields a crossed or
wrong best bid/ask. The current approach per adapter:

| Exchange | WS channel used | Why |
|----------|-----------------|-----|
| binance  | `@depth<N>@100ms` (partial book) | full sorted top-N snapshot (not the `@depth` diff stream) |
| okx      | `books5` | full top-5 snapshot every 100ms (not `books` deltas) |
| bitmex   | `orderBook10` | full top-10 snapshot (not `orderBookL2_25`, whose updates carry no price) |
| bybit    | `orderbook.{depth}` + **state merge** | reconstructs the full book from snapshot+delta frames |
| kraken   | `book` + **state merge** | already reconstructs full state from snapshot+deltas |

On top of that, `operations.stream` computes best bid = `max(bids)` and best
ask = `min(asks)` (ignoring zero-amount levels) defensively, so a momentarily
unsorted book can never surface a crossed pair in the liveness sample. WS-live
order-book `max_depth` is set to the channel's real limit (binance 20, okx 5,
bitmex 10).

## OHLC fidelity

Fields not provided natively are left **null** rather than fabricated (an early
bug filled Coinbase `quote_volume` with `close×volume`). Binance fills
`quote_volume` and `trades` (count); others vary. If you rely on a field, check
the adapter — don't assume parity across exchanges.
