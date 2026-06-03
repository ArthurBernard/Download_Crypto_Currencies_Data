=============
API Reference
=============

dccd v3 follows a hexagonal architecture: a pure, synchronous **domain** with
no I/O, an async **transport** layer, exchange **sources**, **storage**, an
**application** layer of operations, and thin **interfaces** (CLI / HTTP / UI /
Python ``Client``).

Client
======

.. autoclass:: dccd.Client
   :members:

Domain
======

.. automodule:: dccd.domain.symbol
   :members:

.. automodule:: dccd.domain.records
   :members:

.. automodule:: dccd.domain.capability
   :members:

.. automodule:: dccd.domain.timeutils
   :members:

.. automodule:: dccd.domain.transforms
   :members:

Application
===========

.. automodule:: dccd.application.operations
   :members:

.. automodule:: dccd.application.config
   :members:

.. automodule:: dccd.application.scheduler
   :members:

Sources
=======

.. automodule:: dccd.sources.base
   :members:

.. automodule:: dccd.sources.registry
   :members:

Storage
=======

.. automodule:: dccd.storage.parquet
   :members:

.. automodule:: dccd.storage.migrate
   :members:

Transport
=========

.. automodule:: dccd.transport.paginate
   :members:
