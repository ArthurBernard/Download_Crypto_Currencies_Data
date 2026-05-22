#!/usr/bin/env python3
# coding: utf-8

""" Daemon module for autonomous data collection.

.. currentmodule:: dccd.daemon

Submodules
----------

.. autosummary::

   backfill
   config
   health
   storage
   scheduler
   stream_manager

"""

from dccd.daemon.backfill import KrakenBackfill, OHLCBackfill, make_job, run_backfill
from dccd.daemon.config import CollectorConfig, load_config
from dccd.daemon.health import HealthMonitor
from dccd.daemon.scheduler import build_histo_scheduler, run_once
from dccd.daemon.storage import RemoteStorage
from dccd.daemon.stream_manager import StreamManager, SyncService

__all__ = [
    'CollectorConfig',
    'HealthMonitor',
    'KrakenBackfill',
    'OHLCBackfill',
    'load_config',
    'make_job',
    'run_backfill',
    'RemoteStorage',
    'StreamManager',
    'SyncService',
    'build_histo_scheduler',
    'run_once',
]
