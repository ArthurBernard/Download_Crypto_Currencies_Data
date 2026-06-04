===========
Application
===========

The application layer wires domain, sources and storage into the four
operations, plus the orchestration that schedules them and the events they emit.

Operations
==========

``backfill`` / ``stream`` / ``read`` / ``inventory`` are the verbs; everything
else is plumbing. They take their infrastructure (registry, store, events) as
keyword arguments so call sites stay explicit and testable.

.. automodule:: dccd.application.operations
   :members:

Jobs
====

A :class:`~dccd.application.jobs.JobSpec` is a declarative unit of work
(operation + target + trigger + params); the config expands one
:class:`~dccd.application.config.JobConfig` into one spec per pair.

.. automodule:: dccd.application.jobs
   :members:

Scheduler
=========

Routes each spec by trigger kind — supervised → stream worker (auto-reconnect),
interval/cron → periodic backfill, once → one-shot.

.. autoclass:: dccd.application.scheduler.Scheduler
   :members:

Events
======

.. automodule:: dccd.application.events
   :members:

Configuration
=============

The Pydantic config models — :class:`~dccd.application.config.AppConfig` and its
sections — are documented in the :doc:`/configuration` reference.

Service factory
===============

The single place that wires every adapter — edit it to add an exchange.

.. automodule:: dccd.application.service_factory
   :members:
