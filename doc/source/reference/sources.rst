=======
Sources
=======

One adapter per exchange, each implementing the fine-grained ``Source``
protocols and declaring its :class:`~dccd.domain.capability.Capability` set. The
engine resolves an adapter from the registry; you drive them through
:class:`dccd.Client`.

For each exchange's capabilities, quirks and adapter class, see the
:doc:`per-exchange pages </exchanges>`.

Adding an exchange
==================

Implement the relevant protocol mixins below and a ``capabilities()`` method,
then register the adapter in
:func:`~dccd.application.service_factory.build_registry`.

Registry
========

.. autoclass:: dccd.sources.registry.SourceRegistry
   :members:

Source protocols
================

.. automodule:: dccd.sources.base
   :members:
   :show-inheritance:
