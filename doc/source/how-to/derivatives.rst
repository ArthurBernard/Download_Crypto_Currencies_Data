=====================================================
Collect derivative data (funding, open interest, …)
=====================================================

dccd collects three perpetual/futures data types on top of spot: **funding
rates**, **open interest** and **futures OHLC** (continuous contracts). All of
them are addressed with a market suffix on the pair.

The ``:market`` symbol suffix
=============================

A plain pair is spot; append ``:<market>`` to target a derivative market:

========================== ===================================================
Symbol                     Market
========================== ===================================================
``BTC/USDT``               spot (default)
``BTC/USDT:perp``          perpetual future (USDT-margined)
``BTC/USDT:quarter``       current-quarter future (continuous, auto-rolled)
``BTC/USDT:next_quarter``  next-quarter future (continuous, auto-rolled)
========================== ===================================================

The suffix works everywhere a pair does — CLI, config jobs, the web UI's pair
field and the Python API. On disk the market becomes part of the pair
directory, e.g. ``binance/funding/BTC-USDT_PERP/``.

Funding rates
=============

Realized funding events (one row per settlement, typically every 8 h) are
available with **full history** on Binance and Bybit — one backfill drains
everything back to contract launch:

.. code-block:: bash

   dccd backfill -e binance -s BTC/USDT:perp -t funding --start 2020-01-01
   dccd backfill -e bybit   -s BTC/USDT:perp -t funding --start 2020-01-01

Funding has no ``span`` — it is an event series, like trades.

**Kraken Futures** (exchange name ``krakenfutures``, USD-quoted linear perps)
also serves funding history, with two twists:

- **Hourly cadence** — Kraken settles funding every hour, not every ~8 h like
  Binance/Bybit. Per-event rates are therefore roughly 8× smaller; **normalise
  by cadence** (e.g. to a daily or annualised rate) before comparing across
  exchanges. Don't let the smaller magnitude fool you: in a crash the hourly
  tail gets fat — single hourly prints beyond 1e-3 have happened (e.g. SOL on
  2025-10-10).
- **~1-year rolling window** — the API keeps only about one year of history,
  so a one-shot deep backfill is impossible. Start a **recurring job** early
  and let it accumulate history forward; the window rolls.

.. code-block:: bash

   dccd backfill -e krakenfutures -s BTC/USD:perp -t funding

Note the quote: Kraken Futures perps are ``BTC/USD:perp`` (USD-margined), not
``USDT``.

Open interest
=============

Open interest is **span-typed like OHLC** (one point per interval), so a
``span`` is required. Depth differs sharply per exchange:

- **Bybit** serves full history back to the symbol's launch — backfill freely.
- **Binance** serves only the **last 30 days** (hard API cap). History older
  than that is gone forever, so a Binance open-interest job must run as a
  **recurring schedule** that keeps collecting forward — start it early and
  never let it lapse for more than 30 days.

.. code-block:: bash

   # Bybit: deep backfill
   dccd backfill -e bybit -s BTC/USDT:perp -t open_interest --span 3600 --start 2021-01-01

   # Binance: bounded to ~30 days regardless of --start
   dccd backfill -e binance -s BTC/USDT:perp -t open_interest --span 3600

Futures OHLC and the basis
==========================

Binance also serves klines for the continuous **perp** and **quarterly**
contracts — same OHLC pipeline, just a suffixed pair:

.. code-block:: bash

   dccd backfill -e binance -s BTC/USDT:quarter -t ohlc --span 86400 --start 2024-01-01

Kraken Futures serves **deep perp klines** through its charts API — full
history back to contract launch (``PF_`` linear perps launched 2022-03; use it
alongside the funding series above):

.. code-block:: bash

   dccd backfill -e krakenfutures -s BTC/USD:perp -t ohlc --span 3600 --start 2022-04-01

Collecting spot and ``:quarter`` OHLC side by side gives you the **basis**
(annualisable carry): dccd stores the two raw series; compute the spread
research-side, e.g.:

.. code-block:: python

   spot = c.read("binance", "BTC/USDT", "ohlc", span=86400)
   fut  = c.read("binance", "BTC/USDT:quarter", "ohlc", span=86400)
   basis = (fut["close"] - spot["close"]) / spot["close"]

Schedule continuous collection
==============================

Declare recurring jobs in ``config.yml`` and run the daemon (this is what keeps
Binance open interest from falling off the 30-day cliff):

.. code-block:: yaml

   jobs:
     - {exchange: binance, pairs: [BTC/USDT:perp], data_type: funding,
        operation: backfill, trigger_kind: interval, every: 86400, start: last}
     - {exchange: binance, pairs: [BTC/USDT:perp], data_type: open_interest,
        operation: backfill, span: 3600, trigger_kind: interval, every: 21600, start: last}

.. code-block:: bash

   dccd start

From the web UI, the **Historical** page has **Funding** and **Open Interest**
tabs: create a job with a ``:perp`` pair, set its **Schedule** (e.g. daily),
and hit **Run** for the initial backfill. The **Data** page shows the stored
datasets under the same tabs.

See :doc:`/configuration` for the full job schema and :doc:`/exchanges` for
per-exchange capability notes.
