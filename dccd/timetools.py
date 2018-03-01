# coding: utf-8
#!/usr/bin/env python 

import time

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