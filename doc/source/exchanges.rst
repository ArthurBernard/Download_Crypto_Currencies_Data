=========
Exchanges
=========

dccd supports **7 exchanges**. Each adapter declares its
:class:`~dccd.domain.capability.Capability` per data type and transport, and the
engine honours them — an unsupported request is rejected early rather than
silently returning wrong or partial data.

Capabilities
============

You pick a **data type** and an **operation** — **backfill** (download history)
or **stream** (collect live). Each cell lists the data types supported for that
operation; the notes below qualify the limits.

.. list-table::
   :header-rows: 1
   :stub-columns: 1
   :widths: 16 44 40

   * - Exchange
     - Backfill (history)
     - Stream (live)
   * - Binance
     - OHLC · trades · book
     - OHLC · trades · book
   * - Coinbase
     - OHLC (300/req) · book · trades *(recent)*
     - trades
   * - Kraken
     - OHLC *(720 recent)* · trades · book
     - OHLC · trades · book
   * - Bybit
     - OHLC · book
     - OHLC · trades · book
   * - OKX
     - OHLC · trades · book
     - OHLC · trades · book
   * - Bitfinex
     - OHLC · trades · book
     - OHLC · trades
   * - BitMEX
     - OHLC (1m/5m/1h/1d) · trades · book
     - OHLC · trades · book

.. note::

   - **Trades backfill is cursor-paginated** — it drains the full requested
     window, not just the first capped page.
   - *recent* means no deep history through the public API; a deeper request is
     **rejected or clamped early**, never silently truncated. Bybit spot has no
     trade history at all.
   - **Order-book backfill** captures a single point-in-time snapshot (there is
     no historical order book); use a **stream** to record the book over time.
   - The **stream** column lists only channels with a real implementation;
     undeclared channels raise :class:`~dccd.domain.errors.NoCapability`.

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

Per-exchange pages
==================

.. grid:: 2 3 4 4
   :gutter: 2

   .. grid-item-card:: Binance
      :link: binance
      :link-type: doc
   .. grid-item-card:: Coinbase
      :link: coinbase
      :link-type: doc
   .. grid-item-card:: Kraken
      :link: kraken
      :link-type: doc
   .. grid-item-card:: Bybit
      :link: bybit
      :link-type: doc
   .. grid-item-card:: OKX
      :link: okx
      :link-type: doc
   .. grid-item-card:: Bitfinex
      :link: bitfinex
      :link-type: doc
   .. grid-item-card:: BitMEX
      :link: bitmex
      :link-type: doc

.. toctree::
   :hidden:

   binance
   coinbase
   kraken
   bybit
   okx
   bitfinex
   bitmex

Adding an exchange
==================

Add an adapter under ``dccd/sources/`` implementing the relevant ``Source``
protocol mixins and a ``capabilities()`` declaration, then register it in
``dccd.application.service_factory.build_registry``.
