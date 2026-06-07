=======================
Configuration Reference
=======================

The daemon and CLI are driven by a YAML config validated by Pydantic. The field
tables below are generated from the models, so defaults and constraints are
always accurate. Validate a file with:

.. code-block:: bash

   dccd validate --config config.yml

Example
=======

.. code-block:: yaml

   settings:
     data_path: /home/me/data/crypto
     timezone: UTC
     ui_host: 127.0.0.1
     ui_port: 8080
     ui_auth_token: null        # set a token to require Bearer auth on /api/*
     ui_allow_origins: []        # opt-in CORS origins (default: same-origin only)

   storage:
     local_path: /home/me/data/crypto
     remotes: []                 # rclone remotes for sync
     sync_interval: 0

   alerts:
     webhook_url: null
     max_consecutive_errors: 3

   jobs:
     - exchange: binance
       pairs: [BTC/USDT, ETH/USDT]
       data_type: ohlc
       operation: backfill
       span: 60
       trigger_kind: interval
       every: 60
       start: last

     - exchange: binance
       pairs: [BTC/USDT]
       data_type: trades
       operation: stream
       trigger_kind: supervised
       start: last

Schema
======

.. currentmodule:: dccd.application.config

.. autopydantic_model:: AppConfig
   :inherited-members: BaseModel

.. autopydantic_model:: SettingsConfig
   :inherited-members: BaseModel

.. autopydantic_model:: StorageConfig
   :inherited-members: BaseModel

.. autopydantic_model:: RemoteConfig
   :inherited-members: BaseModel

.. autopydantic_model:: AlertConfig
   :inherited-members: BaseModel

.. autopydantic_model:: JobConfig
   :inherited-members: BaseModel
