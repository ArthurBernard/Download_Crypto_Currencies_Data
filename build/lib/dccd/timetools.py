#!/usr/bin/env python 
# -*- coding: utf-8 -*-


import time


""" timetools is a module with TimeTools class to manage some time 
functions with respect to each specific API exchange. 

"""


class TimeTools():
    """ Some functions to manage timestamp and date  
    
    """
    def __init__(self):
        pass
        
    
    def TS_to_date(self, TS, form = '%Y-%m-%d %H:%M:%S'):
        """ Convert timestamp to date in specific format
        
        """
        return time.strftime(form, time.localtime(TS))
        
    
    def date_to_TS(self, date, form = '%Y-%m-%d %H:%M:%S'):
        """ Convert date in specific format to timestamp
        
        """
        return time.mktime(time.strptime(date, form))
        
    
    def TS_to_YMD(self, TS):
        a = time.strftime('%Y %m %d', time.localtime(int(TS))).split(' ')
        return dt.datetime(int(a[0]), int(a[1]), int(a[2]))
        
    
    def str_to_TS(self, string):
        """ Return the equivalent interval time in seconds.
        
        """
        if string.lower() in ['weekly', 'week', '7d', '1w']:
            return 604800
        elif string.lower() in ['daily', 'day', '24h', '1d']:
            return 86400
        elif string.lower() in ['bi-hourly', 'bi-hour', '2h']:
            return 7200
        elif string.lower() in ['hourly', 'hour', '1h', '60min']:
            return 3600
        elif string.lower() in ['half-hourly', 'half-hour', '30min']:
            return 1800
        elif string.lower() in ['5-minute', 'five-minute', '5 minute', 'five minute', '5min']:
            return 300
        elif string.lower() in ['minutely', 'minute', '1min']:
            return 60
        else:
            print('Error, string not understood.\nString must be "minutely", "5 minute", "hourly", "daily" or "weekly".')
        
    
    def TS_to_str(self, TS):
        """ Return interval seconds in a sting
        
        """
        if TS == 60:
            return 'Minutely'
        elif TS == 300:
            return 'Five_Minutely'
        elif TS == 1800:
            return 'Half_Hourly'
        elif TS == 3600:
            return 'Hourly'
        elif TS == 7200:
            return 'Bi_Hourly'
        elif TS == 86400:
            return 'Daily'
        elif TS == 604800:
            return 'Weekly'
        else:
            print('Error, no string correspond to this time in seconds.')
        
    def binance_interval(self, interval):
        """ Return the time interval in the format allow by Binance.
        
        :interval: must be in seconds as 60, 180, 300, 900, 1800, 3600, 
        7200, 14400, 21600, 28800, 43200, 86400, 259200, 604800, 2592000.
        
        """
        if interval / 60 in [1, 3, 5, 15, 30]:
            return '{}m'.format(int(interval / 60))
        elif interval / 3600 in [1, 2, 4, 6, 8, 12]:
            return '{}h'.format(int(interval / 3600))
        elif interval / 86400 in [1, 3]:
            return '{}d'.format(int(interval / 86400))
        elif interval == 604800:
            return '1w'
        elif interval == 2592000:
            return '1M'
        else:
            print('No format allowed.')