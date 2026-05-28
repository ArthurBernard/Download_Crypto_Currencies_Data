===============
Getting Started
===============

Installation
------------

Core package (historical REST + WebSocket streaming):

.. code-block:: bash

   pip install dccd

With daemon extras (CLI scheduler, APScheduler, YAML config, rclone sync):

.. code-block:: bash

   pip install dccd[daemon]

From source:

.. code-block:: bash

   git clone https://github.com/ArthurBernard/Download_Crypto_Currencies_Data.git
   cd Download_Crypto_Currencies_Data
   pip install -e ".[daemon,dev]"

.. note::

   All REST endpoints used by ``dccd`` are public — no API key is required.

----

Quickstart: Historical API
--------------------------

Download OHLCV candles via REST, save to Parquet, and load as a DataFrame:

.. code-block:: python

   from dccd.histo_dl import FromBinance

   obj = FromBinance('/data/crypto/', 'BTC', span=3600, fiat='USDT')

   # Full date range
   obj.import_data(start='2024-01-01 00:00:00', end='2024-12-31 00:00:00')
   obj.save(form='parquet')

   df = obj.get_data()
   print(df.head())

All five exchange classes share the same interface: :class:`~dccd.histo_dl.binance.FromBinance`,
:class:`~dccd.histo_dl.kraken.FromKraken`, :class:`~dccd.histo_dl.coinbase.FromCoinbase`,
:class:`~dccd.histo_dl.bybit.FromBybit`, :class:`~dccd.histo_dl.okx.FromOKX`.

See :doc:`histo_dl` for the full API reference.

----

Incremental updates
-------------------

Pass ``start='last'`` to resume from the last saved timestamp, with no
duplicate rows:

.. code-block:: python

   # On first run downloads everything; on subsequent runs resumes from
   # the most recent saved candle.
   obj.import_data(start='last', end='now').save(form='parquet')

This is the recommended pattern for scheduled collection: run it daily or
hourly via cron or the :doc:`daemon`.

----

Trades and Order Book
---------------------

The same fluent API works for trade history and order book snapshots:

.. code-block:: python

   from dccd.histo_dl import FromKraken, FromBinance

   # Trade history — full historical pagination (Kraken, OKX)
   obj = FromKraken('/data/crypto/', 'BTC', span=3600, fiat='USD')
   obj.import_trades(start='2024-01-01 00:00:00', end='2024-12-31 00:00:00')
   obj.save_trades(form='parquet')

   # Order book snapshot (depth = number of price levels per side)
   obj2 = FromBinance('/data/crypto/', 'BTC', span=3600, fiat='USDT')
   obj2.import_orderbook(depth=20)
   obj2.save_orderbook(form='parquet')

.. note::

   **Bybit and Coinbase** return only recent trades (≤ 1 000 and ≤ 100
   respectively) — their public REST APIs do not support deep historical
   pagination.  Use :class:`~dccd.histo_dl.kraken.FromKraken` or
   :class:`~dccd.histo_dl.okx.FromOKX` for full trade history.

----

Output formats
--------------

The ``form`` parameter of ``.save()`` / ``.save_trades()`` controls the file
format.  ``get_data()`` returns a :class:`polars.DataFrame` by default.

.. list-table::
   :header-rows: 1
   :widths: 15 15 70

   * - ``form=``
     - Extension
     - When to use
   * - ``'parquet'``
     - ``.parquet``
     - Recommended — columnar, compressed, fastest for Polars/Pandas loads.
   * - ``'csv'``
     - ``.csv``
     - Universal export, readable without libraries, largest file size.
   * - ``'xlsx'``
     - ``.xlsx``
     - Excel-compatible, good for small datasets.
   * - ``'sqlite'``
     - ``.db``
     - Local SQL queries via SQLAlchemy.
   * - ``'sql'``
     - —
     - Remote database (PostgreSQL, MySQL, …) via SQLAlchemy connection string.

To get a :class:`pandas.DataFrame` instead of the default Polars output:

.. code-block:: python

   df_polars = obj.get_data()               # polars.DataFrame (default)
   df_pandas = obj.get_data(format='pandas') # pandas.DataFrame

----

Quickstart: Continuous (WebSocket) API
---------------------------------------

Stream order book and trades from Binance for one hour, saving a snapshot
every 60 seconds:

.. code-block:: python

   from dccd.continuous_dl import get_data_binance

   get_data_binance(
       path='/data/crypto/',
       pair='BTCUSDT',
       time_step=60,   # snapshot interval in seconds
       until=3600,     # total duration in seconds
       form='parquet',
   )

For fine-grained control use the downloader class directly:

.. code-block:: python

   from dccd.continuous_dl import DownloadBinanceData
   from dccd.tools.io import IODataBase

   dl = DownloadBinanceData(pair='BTCUSDT', time_step=60, until=3600)
   dl.set_trades_saver(IODataBase('/data/crypto/trades', method='parquet'))
   dl.set_book_saver(IODataBase('/data/crypto/book', method='parquet'))
   dl.run()

See :doc:`continuous_dl` for all six exchange classes.

----

Quickstart: CLI Daemon
-----------------------

Create a minimal config file ``config.yml``:

.. code-block:: yaml

   storage:
     local_path: /data/crypto

   histo_jobs:
     - exchange: binance
       pairs: [BTC/USDT, ETH/USDT]
       span: 3600

   stream_jobs:
     - exchange: binance
       pairs: [BTC/USDT]
       channels: [trades, book]
       time_step: 60

Validate the config, backfill history, then start the daemon:

.. code-block:: bash

   dccd validate --config config.yml
   dccd backfill --config config.yml
   dccd start   --config config.yml

Check the status of running jobs:

.. code-block:: bash

   dccd status --config config.yml

See :doc:`daemon` for the full daemon reference including remote sync,
health monitoring, and all CLI commands.
