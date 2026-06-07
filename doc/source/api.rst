=============
API Reference
=============

dccd v3 is a hexagonal architecture (see :doc:`architecture`): a pure **domain**,
an async **transport** layer, exchange **sources**, **storage**, an
**application** layer of operations, and thin **interfaces**. The reference is
organised by layer — each page introduces the layer, then documents its members.

Most users only need the :doc:`reference/client`.

.. grid:: 1 2 2 3
   :gutter: 3

   .. grid-item-card:: Client
      :link: reference/client
      :link-type: doc

      The one-stop async facade.

   .. grid-item-card:: Domain
      :link: reference/domain
      :link-type: doc

      Pure value objects, capabilities and transforms — no I/O.

   .. grid-item-card:: Transport
      :link: reference/transport
      :link-type: doc

      Async HTTP, WebSocket base, rate limiter and paginators.

   .. grid-item-card:: Sources
      :link: reference/sources
      :link-type: doc

      The ``Source`` protocols and the adapter registry.

   .. grid-item-card:: Storage
      :link: reference/storage
      :link-type: doc

      Parquet datasets and run history.

   .. grid-item-card:: Application
      :link: reference/application
      :link-type: doc

      Operations, scheduler, jobs, events and config.

.. toctree::
   :hidden:

   reference/client
   reference/domain
   reference/transport
   reference/sources
   reference/storage
   reference/application
