=======
Storage
=======

.. currentmodule:: dccd.storage

The storage layer persists every data type to **Parquet** with nanosecond
timestamps, provenance and per-type deduplication, and keeps an append-only run
history in SQLite. :class:`~dccd.storage.parquet.ParquetStore` is the unified
read/write interface; you rarely touch it directly — :class:`dccd.Client` and the
operations use it for you.

Directory layout
================

::

    {data_path}/{exchange}/ohlc/{pair}/{span}/YYYY.parquet
    {data_path}/{exchange}/trades/{pair}/YYYY-MM-DD.parquet
    {data_path}/{exchange}/orderbook/{pair}/YYYY-MM-DD.parquet

- *exchange* — lowercase (``binance``, ``kraken``, …).
- *pair* — ``BTC-USDT`` (slash replaced by hyphen).
- *span* — seconds label (``3600s``); OHLC only.
- OHLC files are **annual**; trades and order-book files are **daily**.

Schema & integrity
==================

All timestamps are ``TS`` — **nanoseconds UTC** (``int64``). Each write merges
into the existing file and deduplicates on the **natural key** per data type:

.. list-table::
   :header-rows: 1
   :widths: 20 35 45

   * - Data type
     - Dedup key
     - Notes
   * - OHLC
     - ``TS``
     - One bar per span window.
   * - trades
     - ``tid`` (else ``TS, price, amount, side``)
     - Many trades share a ``TS`` — keying on ``TS`` alone would lose them.
   * - order book
     - ``TS, side, price``
     - A snapshot's levels all share one ``TS``.

Writes are **atomic** (temp file + ``os.replace``) and serialised per file, so a
reader never sees a half-written Parquet and concurrent writers can't corrupt it.

Reading data
============

.. code-block:: python

   from dccd.storage.parquet import ParquetStore
   from dccd.domain.dataset import DatasetId
   from dccd.domain.symbol import Symbol
   from dccd.domain.types import DataType

   store = ParquetStore("/data/crypto")
   ds = DatasetId(exchange="binance", symbol=Symbol(base="BTC", quote="USDT"),
                  data_type=DataType.OHLC, span=3600)
   df = store.load(ds)          # a polars.DataFrame, sorted by TS

ParquetStore
============

.. autoclass:: dccd.storage.parquet.ParquetStore
   :members:

Run history
===========

.. autoclass:: dccd.storage.runs_sqlite.RunsStore
   :members:
