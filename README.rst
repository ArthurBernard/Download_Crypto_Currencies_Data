=============================
Download Crypto-Currency Data
=============================

.. image:: https://img.shields.io/pypi/pyversions/dccd
    :alt: PyPI - Python Version

.. image:: https://img.shields.io/pypi/v/dccd.svg
    :target: https://pypi.org/project/dccd/
    :alt: PyPI

.. image:: https://github.com/ArthurBernard/Download_Crypto_Currencies_Data/actions/workflows/ci.yml/badge.svg
    :target: https://github.com/ArthurBernard/Download_Crypto_Currencies_Data/actions/workflows/ci.yml
    :alt: CI

.. image:: https://img.shields.io/github/license/ArthurBernard/Download_Crypto_Currencies_Data.svg
    :target: https://github.com/ArthurBernard/Download_Crypto_Currencies_Data/blob/master/LICENSE.txt
    :alt: License

.. image:: https://readthedocs.org/projects/download-crypto-currencies-data/badge/?version=latest
    :target: https://download-crypto-currencies-data.readthedocs.io/en/latest/
    :alt: Documentation Status

.. image:: https://pepy.tech/badge/dccd
    :target: https://pepy.tech/project/dccd
    :alt: Downloads

Python package to download crypto-currency data (OHLCV, trades, order book) from multiple
exchanges via REST and WebSocket APIs. Data can be saved to CSV, Excel, SQLite, PostgreSQL,
or Parquet.

Installation
============

From pip::

    $ pip install dccd

With optional Parquet / Polars support::

    $ pip install "dccd[io]"

From source::

    $ git clone https://github.com/ArthurBernard/Download_Crypto_Currencies_Data
    $ cd Download_Crypto_Currencies_Data
    $ pip install -e .

Requirements
============

- Python >= 3.10
- numpy >= 1.26
- pandas >= 2.0
- requests >= 2.28
- openpyxl >= 3.1
- websockets >= 12.0
- scipy >= 1.10
- SQLAlchemy >= 2.0
- tenacity >= 8.0
- pydantic >= 2.0

Presentation
============

The ``dccd`` package provides two main download methods:

**Historical Downloader** ``dccd.histo_dl``
    Download historical OHLCV data via REST APIs and save to disk.
    Supports **Binance**, **Coinbase**, **Kraken**, **Bybit**, **OKX**.

**Continuous Downloader** ``dccd.continuous_dl``
    Stream real-time data (order book, trades) via WebSocket and update a database
    continuously. Supports **Bitfinex**, **Bitmex**, **Bybit**.

Quick start
===========

Historical data::

    from dccd.histo_dl import FromBinance

    obj = FromBinance('/path/to/data/', 'BTC', 3600)
    obj.import_data(start='2024-01-01 00:00:00', end='2024-12-31 00:00:00')
    obj.save(form='parquet')
    df = obj.get_data()

Polars output::

    df_pl = obj.get_data(format='polars')

Links
=====

- PyPI: https://pypi.org/project/dccd/
- Documentation: https://download-crypto-currencies-data.readthedocs.io/
- Source: https://github.com/ArthurBernard/Download_Crypto_Currencies_Data
- Changelog: https://github.com/ArthurBernard/Download_Crypto_Currencies_Data/blob/master/CHANGELOG.md
