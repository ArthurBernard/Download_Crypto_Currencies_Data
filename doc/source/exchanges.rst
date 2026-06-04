=========
Exchanges
=========

dccd supports **7 exchanges**. Each adapter declares its
:class:`~dccd.domain.capability.Capability` per data type and transport, and the
engine honours them — an unsupported request is rejected early rather than
silently returning wrong or partial data.

Capabilities
============

.. list-table::
   :header-rows: 1
   :stub-columns: 1

   * - Exchange
     - OHLC REST
     - Trades REST
     - Book REST
     - WebSocket (live)
   * - Binance
     - ✅ full
     - ✅ full
     - ✅
     - OHLC · trades · book
   * - Coinbase
     - ✅ full (300/req)
     - ⚠️ recent only
     - ✅
     - trades
   * - Kraken
     - ⚠️ 720 recent
     - ✅ full
     - ✅
     - OHLC · trades · book
   * - Bybit
     - ✅ full
     - ❌ no spot history
     - ✅
     - OHLC · trades · book
   * - OKX
     - ✅ full
     - ✅ full
     - ✅
     - OHLC · trades · book
   * - Bitfinex
     - ✅ full
     - ✅ full
     - ✅
     - OHLC · trades
   * - BitMEX
     - ✅ full (4 spans)
     - ✅ full
     - ✅
     - OHLC · trades · book

.. note::

   **Trades REST is cursor-paginated** — a backfill drains the full requested
   window, not just the first capped page. ``⚠️ recent only`` means the exchange
   exposes no deep trade history through the public JSON API (a deep request is
   rejected early, not silently truncated). The **WebSocket** column lists only
   channels with a real implementation; undeclared channels raise
   :class:`~dccd.domain.errors.NoCapability`.

OHLC field fidelity
===================

Not every exchange returns every OHLC field natively. Missing fields are stored
as ``null`` — never fabricated.

.. list-table::
   :header-rows: 1
   :stub-columns: 1

   * - Exchange
     - ``quote_volume``
     - ``trades`` (count)
   * - Binance
     - ✅ native
     - ✅ native
   * - Bybit / OKX
     - ✅ native
     - — null
   * - Kraken
     - ✅ vwap × volume (exact)
     - ✅ native
   * - Coinbase / Bitfinex / BitMEX
     - — null
     - — null

Per-exchange notes
==================

- **Binance** — full history (klines, aggTrades, depth 5000); the reference
  implementation for cursor pagination.
- **Coinbase** — 300 candles/request (windowed automatically); trades are recent
  only (header cursors are not exposed by the JSON transport).
- **Kraken** — OHLC REST serves the 720 most recent bars (``history="recent"``):
  a deep request is **clamped to that window with a warning**. Trades are full
  history via the ``since`` cursor.
- **Bybit** — full OHLC; spot has **no trade history** (WS only) → declared as
  :class:`~dccd.domain.errors.NoCapability` for trades backfill.
- **OKX** — deep history via ``history-candles`` / ``history-trades``.
- **Bitfinex** — up to 10 000 items/request. Tether is labelled ``UST``, so
  ``BTC/USDT`` maps to ``tBTCUST`` automatically.
- **BitMEX** — bucketed OHLC (1m/5m/1h/1d only); full trade history.

Adding an exchange
==================

Add an adapter under ``dccd/sources/`` implementing the relevant ``Source``
protocol mixins and a ``capabilities()`` declaration, then register it in
``dccd.application.service_factory.build_registry``.
