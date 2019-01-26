#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Simple exemple how to use the 'download crypto currencies data' 
package. 

The class 'FromKraken' download data from the kraken exchange, 
similarly FromExchange download data from exchange. For each exchange 
the same methods are allow but sometime different parameters.

Initialization with some parameters are a 'path' where to save data, 
a 'crypto-currencies' of your choice, and an 'intervall time' (refer 
to docstring of each classes to know which intervall is allow to each 
echange). Optional parameters are a 'fiat currency' (default is 'USD') 
but can also be a crypto-currency, and a 'format' how to save the data 
(only xlsx is allow for the moment).

The method 'import_data' download data from the exchange, parameters 
are a 'start' the first observation (timestamp) and an 'end' the last 
observation (timestamp) save the data in the specific folder and 
format. 
If start is the string 'last' the programm try to find the last data 
download if it exist and update the data base. 
If end is the string 'now' end will be the current time.
start and end can also be date format 'yyyy-mm-dd hh:mm:ss' as string.

The method 'save' without parameter saves the data downloaded.

The method 'get_data' without parameter returns the data downloaded as 
pd.DataFrame.

"""

import time

import pandas as pd

from dccd import FromPoloniex as pk

xbtusd = pk('/home/arthur/Data/Crypto_Currencies/', 'XBT', 86400, fiat='USD')

start = '2018-03-01 00:00:00' # date format 'yyyy-mm-dd hh:mm:ss' as string
end = time.time() - 86400 * 5 # date format timestamp of 5 days before today

xbtusd.import_data(start=start, end=end).save(form='csv').get_data()

xbtusd.import_data(start='last', end='now').save(form='xlsx').get_data() # update data base