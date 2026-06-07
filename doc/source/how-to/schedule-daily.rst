=========================
Schedule daily collection
=========================

Declare interval/stream jobs in ``config.yml`` and run the daemon; it schedules
backfills and supervises streams (auto-reconnect):

.. code-block:: yaml

   jobs:
     - {exchange: binance, pairs: [BTC/USDT, ETH/USDT], data_type: ohlc,
        operation: backfill, span: 3600, trigger_kind: interval, every: 3600, start: last}
     - {exchange: binance, pairs: [BTC/USDT], data_type: trades,
        operation: stream, trigger_kind: supervised, start: last}

.. code-block:: bash

   dccd start

See :doc:`/configuration` for the full job schema.
