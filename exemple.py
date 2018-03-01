#!/usr/bin/env python

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

The method 'save' without parameter save the data downloaded.

The method 'show' without parameter show the data downloaded.
"""

#!/usr/bin/env python

import time

from dccd.dl_cc_data import FromPoloniex as pk

xbtusd = pk('D:/Users/Arthur/Crypto_Currencies/', 'XBT', 86400, fiat='USD')

start = time.time()-86400*10 # start 10 days before today
end = time.time()-86400*5 # stop 5 days before today

xbtusd.import_data(start=start, end=end).save().show()

xbtusd.import_data(start='last', end='now').save().show() # update data base

input('press ENTER to close')
exit()