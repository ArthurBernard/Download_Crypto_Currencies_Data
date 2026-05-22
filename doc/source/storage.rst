------------------------------------------
 Storage (:mod:`dccd.storage`)
------------------------------------------

.. automodule:: dccd.storage
   :no-members:
   :no-inherited-members:
   :no-special-members:

:class:`DataStore` is the unified read/write interface for all dccd data
types (OHLC, trades, order book).  It replaces the scattered save/load
logic previously spread across ``exchange.py``, ``backfill.py``, and
``stream_manager.py``.

Directory layout
----------------

::

    {data_path}/{exchange}/ohlc/{pair}/{span}/YYYY.parquet
    {data_path}/{exchange}/trades/{pair}/YYYY-MM-DD.parquet
    {data_path}/{exchange}/orderbook/{pair}/YYYY-MM-DD.parquet

- *exchange*: lowercase (``'binance'``, ``'kraken'``…)
- *pair*: ``BTC-USDT`` (slash replaced by hyphen)
- *span*: short label — ``'1m'``, ``'1h'``, ``'1d'``… (OHLC only)
- OHLC files are **annual**; trades and orderbook files are **daily**

Example paths::

    ~/data/crypto/binance/ohlc/BTC-USDT/1h/2026.parquet
    ~/data/crypto/kraken/ohlc/BTC-USD/1d/2025.parquet
    ~/data/crypto/binance/trades/BTC-USDT/2026-05-22.parquet
    ~/data/crypto/binance/orderbook/BTC-USDT/2026-05-22.parquet

API
---

.. autosummary::
   :toctree: generated/

   DataStore
