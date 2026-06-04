==================
Add a new exchange
==================

Implement an adapter under ``dccd/sources/`` with the relevant ``Source``
protocol mixins and a ``capabilities()`` declaration, then register it in
``dccd.application.service_factory.build_registry``. See :doc:`/architecture`.
