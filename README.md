# Download Crypto-Currency Data

![PyPI - Python Version](https://img.shields.io/pypi/pyversions/dccd)
[![PyPI](https://img.shields.io/pypi/v/dccd.svg)](https://pypi.org/project/dccd/)
[![Status](https://img.shields.io/pypi/status/dccd.svg?colorB=blue)](https://pypi.org/project/dccd/)
[![Build Status](https://travis-ci.org/ArthurBernard/Download_Crypto_Currencies_Data.svg?branch=master)](https://travis-ci.org/ArthurBernard/Download_Crypto_Currencies_Data)
[![license](https://img.shields.io/github/license/ArthurBernard/Download_Crypto_Currencies_Data.svg)](https://github.com/ArthurBernard/Download_Crypto_Currencies_Data/blob/master/LICENSE.txt)
[![Downloads](https://pepy.tech/badge/dccd)](https://pepy.tech/project/dccd)
[![Documentation Status](https://readthedocs.org/projects/download-crypto-currencies-data/badge/?version=latest)](https://download-crypto-currencies-data.readthedocs.io/en/latest/?badge=latest)
[![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/ArthurBernard/Download_Crypto_Currencies_Data.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/ArthurBernard/Download_Crypto_Currencies_Data/context:python)

#### This is the starting point of a python package to automatically *download* data and *update* database with *crypto-currency* data (bitcoin, ether, litecoin, etc.) from different API exchanges (allow only Binance, Bitmex, Bitfinex, GDAX, Kraken and Poloniex for the moment).

## Presentation:

The ``dccd`` package allow you two main methods to download data. The first one is recommended to download data at high frequency (**minutely** or **tick by tick**), and the second one is recommended to download data at a lower frequency (**hourly** or **daily**):

- **Continuous Downloader `dccd.continuous_dl`**:   
   Download and update continuously data (orderbook, trades tick by tick, ohlc, etc) and save it in a database. *Currently only support Bitfinex and Bitmex exchanges*.

- **Historical Downloader `dccd.histo_dl`**:   
   Download historical data (ohlc, trades, etc.) and save it. *Currently only support Binance, GDax, Kraken and Poloniex exchanges*.

### Historical Downloader:

The **'dccd.histo_dl'** module contains a main class and four classes to download and update data for each exchange, and a module **'date_time'** to manage specific time format needed for each API.

The four classes to download historical data are **FromBinance**, **FromGDax**, **FromKraken** and **FromPoloniex**. All have the same methods and almost the same parameters:    

- **\_\_init\_\_(path, crypto, span, fiat(optional), form(optional))** initialisation with **path** is the path where save the data (string), **crypto** is a crypto currency (string) and **span** is the interval time between each observation in seconds (integer) or can be a string as 'hourly', 'daily', etc. (see details on the doc string). The optional parameters are **fiat** the second currency (default is 'USD' and 'USDT' for poloniex and binance) and **form** the format to save the data (default is 'xlsx').    

- **import\_data(start, end)** download data with **start** and **end** the timestamp (integer) or the date and time (string as 'yyyy-mm-dd hh:mm:ss'), respectively of the first observation and the last observation (default are special parameters **start='last'** allow the last data saved and **end='now'** allow the last observation available). Exclusion: Kraken don't allow the **end** parameter and provide only the thousand last observations.    

- **save(form(optional), by(optional))** save the data with **form** the format of the saved data (default is 'xlsx') and **by** is the "size" of each saved file (default is 'Y' as an entire year). Exclusion: This optional parameters are in progress, let the default parameter for the moment, other are not allow.    

- **get_data()** returns the data frame without any parameter.    

Method chaining is available.

## Installation:

#### Install the library from pip:

> $ pip install dccd

#### Install the library from source:

> $ git clone https://github.com/ArthurBernard/Download_Crypto_Currencies_Data   
> $ cd Download_Crypto_Currencies_Data   
> $ python setup.py install --user

## Requirements:

- numpy>=1.14.1   
- pandas>=0.22.0   
- requests>=2.18.4   
- xlrd>=1.1.0   
- xlsxwriter>=1.0.2   
- websockets>=7.0.0   
- scipy>=1.2.0   
- SQLAlchemy>=1.3.0   

## Supported exchanges:

- **Binance.**

- **Bitfinex.**

- **Bitmex.**

- **GDAX.**

- **Kraken.**

- **Poloniex.**


***
*Package not achieved, always in progress. This is my first one package, all advice is welcome.*
