#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""The 'dccd' package contain the module 'dl_cc_data' with 5 classes to 
download and update data for each exchange, and a module 'timetools' to 
manage specific time functions needed for each API.

The four classes to download data are FromBinance, FromGDAX, FromKraken 
and FromPoloniex. All have the same methods and almost the same 
parameters:

__init__(path, crypto, span, fiat(optional), form(optional)): 
initialisation with path is the path where save the data (string), 
crypto is a crypto currency (string) and span is the interval time 
between each observation in seconds (integer) or can be a string as 
'hourly', 'daily', etc. (see details on the doc string). The optional 
parameters are fiat the second currency (default is 'USD' and 'USDT' 
for poloniex and binance) and form the format to save the data (default 
is 'xlsx').

import_data(start, end): download data with start and end the timestamp 
(integer) or the date and time (string as 'yyyy-mm-dd hh:mm:ss'), 
respectively of the first observation and the last observation (default 
are special parameters start='last' allow the last data saved and 
end='now' allow the last observation available). Exclusion: Kraken 
don't allow the end parameter and provide only the thousand last 
observations.

save(form(optional), by(optional)): save the data with form the format 
of the saved data (default is 'xlsx') and by is the "size" of each 
saved file (default is 'Y' as an entire year). Exclusion: This optional 
parameters are in progress, let the default parameter for the moment, 
other are not allow.

show(): print the data frame without any parameter.

Method chaining is available.

"""

__version__ = "0.1.1"

from .dl_cc_data import FromBinance, FromGDAX, FromKraken, FromPoloniex