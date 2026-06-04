======
Domain
======

The domain is pure and synchronous — value objects and transforms with **no
I/O**. Every timestamp is **nanoseconds UTC** (``int64``). Exchange payloads are
validated into these Pydantic models before anything is stored, so corrupt data
never reaches Parquet.

Records at a glance
===================

.. list-table::
   :header-rows: 1
   :widths: 22 28 50

   * - Model
     - Key fields
     - Notes
   * - :class:`~dccd.domain.records.OHLCBar`
     - ``ts, open, high, low, close, volume``
     - ``quote_volume`` / ``trades`` may be null (see :doc:`/exchanges`).
   * - :class:`~dccd.domain.records.Trade`
     - ``ts, price, amount, side, tid``
     - ``side`` ∈ {buy, sell}; ``tid`` is the dedup key.
   * - :class:`~dccd.domain.records.OrderBookSnapshot`
     - ``ts, bids, asks, is_snapshot``
     - ``is_snapshot=False`` is an incremental delta.

Symbol & types
==============

.. automodule:: dccd.domain.symbol
   :members:

.. automodule:: dccd.domain.types
   :members:

Records
=======

.. automodule:: dccd.domain.records
   :members:

Capabilities
============

A :class:`~dccd.domain.capability.Capability` is what an adapter declares it can
do for a ``(data_type × transport × mode)``; the engine resolves against it and
raises :class:`~dccd.domain.errors.NoCapability` early. See :doc:`/architecture`.

.. automodule:: dccd.domain.capability
   :members:

.. automodule:: dccd.domain.dataset
   :members:

.. automodule:: dccd.domain.errors
   :members:

Transforms
==========

.. automodule:: dccd.domain.transforms
   :members:
