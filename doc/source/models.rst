------------------------------------
 Data Models (:mod:`dccd.models`)
------------------------------------

.. automodule:: dccd.models
   :no-members:
   :no-inherited-members:
   :no-special-members:

All raw exchange payloads are validated through Pydantic v2 models before
being stored.  If a response field has an unexpected type (e.g. a non-numeric
price string) the model raises a ``ValidationError`` immediately, so corrupt
data never silently reaches the Parquet files.

Models at a glance
------------------

**OHLCBar** — one OHLCV candle returned by REST endpoints:

.. list-table::
   :header-rows: 1
   :widths: 20 15 65

   * - Field
     - Type
     - Notes
   * - ``date``
     - float
     - Unix timestamp in seconds.
   * - ``open``, ``high``, ``low``, ``close``
     - float
     - OHLC prices.
   * - ``volume``
     - float
     - Base-asset volume.
   * - ``quoteVolume``
     - float
     - Quote-asset volume (price × volume).
   * - ``weightedAverage``
     - float | None
     - VWAP.  Populated by Kraken only; ``None`` for all other exchanges.

**Trade** — one individual trade from REST history or WebSocket streams:

.. list-table::
   :header-rows: 1
   :widths: 20 15 65

   * - Field
     - Type
     - Notes
   * - ``tid``
     - int | None
     - Exchange trade ID.  ``None`` when the exchange does not provide an
       integer ID (e.g. Kraken, Bybit).
   * - ``timestamp``
     - float
     - Unix timestamp in seconds.
   * - ``price``
     - float
     - Trade price.
   * - ``amount``
     - float
     - Trade size in base asset.
   * - ``type``
     - str | None
     - ``'buy'`` or ``'sell'``.  ``None`` when the side is not reported.

**OrderBookEntry** — one price level from a snapshot or diff stream:

.. list-table::
   :header-rows: 1
   :widths: 20 15 65

   * - Field
     - Type
     - Notes
   * - ``side``
     - str
     - ``'bid'`` or ``'ask'``.
   * - ``price``
     - str
     - Price level stored as a string to preserve full exchange precision.
   * - ``amount``
     - float
     - Total quantity available at this level.
   * - ``count``
     - int | None
     - Number of open orders.  ``None`` when not provided (e.g. Binance,
       Kraken).

API reference
-------------

.. autosummary::
   :toctree: generated/

   OHLCBar
   Trade
   OrderBookEntry
