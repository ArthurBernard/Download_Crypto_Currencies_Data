=======================================
Analyse stored data in Polars or Pandas
=======================================

:meth:`~dccd.Client.read` returns a Polars DataFrame; convert if you prefer
Pandas:

.. code-block:: python

   df = c.read("binance", "BTC/USDT", "ohlc", span=3600)
   pdf = df.to_pandas()

Or read the Parquet files directly with any tool —
``{data_path}/{exchange}/ohlc/{pair}/{span}/{year}.parquet``.
