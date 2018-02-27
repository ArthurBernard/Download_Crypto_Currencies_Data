# coding: utf-8
#!/usr/bin/env python3 

import pandas as pd
import numpy as np
import time
import pickle
import json
import requests
import pathlib
import os

class TimeFunction():
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


class ImportDataCryptoCurrencies():
    """ Class to import data about crypto-currencies from some exchanges 
        platform. Don't use directly this class, use the respective class 
        for each exchange.
    
    """
    def __init__(self, path, crypto, span, platform, fiat = 'EUR', 
                 form = 'xlsx'):
        """ Initialisation.
        
            path :     The path where data will be save.
            crypto :   The abreviation of the crypto-currencies as string.
            span :     As string for 'weekly', 'daily', 'hourly', or the integer 
                       of the seconds between each observations.
            platform : The platform of your choice as string : 'Kraken', 'Poloniex'
            start :    Timestamp of the first observation of you want, as integer.
            end :      Timestamp of the last observation of you want as integer.
            fiat :     Basically the fiat as you want, but can also be an 
                       crypto-currencies.
            form :     As string, your favorit format e.g. 'csv', 'txt', 'xlsx'.
            
        """
        self.path = path
        self.crypto = crypto
        self.span, self.per = self.period(span)
        self.fiat = fiat
        self.pair = crypto+fiat
        self.full_path = self.path+'/'+platform+'/Data/Clean_Data/'+str(self.per)+'/'+str(self.pair)
        self.last_df = pd.DataFrame()
        
    
    def last_date(self):
        """ Find the last observation imported.
        to finish
        """
        pathlib.Path(self.full_path).mkdir(parents=True, exist_ok=True)
        if not os.listdir(self.full_path):
            return 1325376000
        else: 
            last_file = sorted(os.listdir(self.full_path), reverse = True)[0]
            print(last_file.split('.')[-1])
            if last_file.split('.')[-1] == 'xlsx':
                self.last_df = pd.read_excel(self.full_path+'/'+str(last_file))
                return self.last_df.TS.iloc[-1]
            else:
                print('Last saved file is in format not allowing. Start at 1st January 2012.')
                return 1325376000
        
    
    def time(self, start, end):
        """ Determine the end and start.
        to finish
        """
        if start is 'last':
            start = self.last_date()
        else:
            pass
        if end is 'now':
            end = time.time()
        else:
            pass
        return int((start//self.span)*self.span), int((end//self.span)*self.span)
        
    
    def by_period(self, TS):
        return TimeFunction().TS_to_date(TS, form = '%'+self.by)
        
    
    def name_file(self, date):
        return self.per+'_of_'+self.crypto+self.fiat+'_in_'+date
        
    
    def save(self, form = 'xlsx', by = 'Y'):
        """ Save data by period (default is year) in the corresponding format and file
        to finish
        """
        df = (self.last_df.append(self.df)
              .drop_duplicates(subset='TS', keep='last')
              .reset_index(drop=True)
              .drop('Date', axis = 1)
              .reindex(columns = ['TS', 'date', 'time', 'close', 'high', 'low', 'open', 
                                  'quoteVolume', 'volume', 'weightedAverage']))
        pathlib.Path(self.full_path).mkdir(parents=True, exist_ok=True) 
        self.by = by
        grouped = df.set_index('TS', drop = False).groupby(self.by_period, axis = 0)#.reset_index()
        for name, group in grouped:
            if form is 'xlsx':
                writer = pd.ExcelWriter(self.full_path+'/'+self.name_file(name)+'.'+form, engine='xlsxwriter')
                df_group = group.reset_index(drop=True)
                df_group.to_excel(writer, header = True, index = False, sheet_name='Sheet1')
                work_book = writer.book
                work_sheet = writer.sheets['Sheet1']
                fmt = work_book.add_format({'align': 'center', 'num_format': '#,##0.00'})
                fmt_time = work_book.add_format({'align': 'center', 'num_format': 'hh:mm:ss'})
                fmt_date = work_book.add_format({'align': 'center', 'num_format': 'yyyy/mm/dd'})
                fmt_TS = work_book.add_format({'align': 'center'})
                work_sheet.set_column('A:A', 11, fmt_TS)
                work_sheet.set_column('B:B', 10, fmt_date)
                work_sheet.set_column('C:C', 10, fmt_time)
                work_sheet.set_column('J:J', 17, fmt)
                work_sheet.set_column('D:I', 13, fmt)
                writer.save()
            else:
                print('Not allowing fomat')
        return self
        
    
    def sort_data(self, data):
        """ Clean and sort the data.
        to finish
        """
        df = pd.DataFrame(data).rename(columns = {'date' : 'TS'})#, index = range(self.start, self.end, self.span))
        TS = pd.DataFrame(list(range(self.start, self.end, self.span)), columns = ['TS'])
        df = (df.merge(TS, on = 'TS', how ='outer')
              .sort_values('TS')
              .reset_index(drop=True)
              .fillna(method = 'pad'))
        df = df.assign(Date = pd.to_datetime(df.TS, unit='s'))
        self.df = df.assign(date = df.Date.dt.date, time = df.Date.dt.time)
        return self
        
    
    def show(self):
        """ Print the datafram
        
        """
        return self.df
    
    
    def period(self, span):
        if type(span) is str:
            return TimeFunction().str_to_TS(span), span
        elif type(span) is int:
            return span, TimeFunction().TS_to_str(span)
        else:
            print("Error, span don't have the appropiate format as string or integer (seconds)")
        
    
    def specific_hierarchy(self, liste):
        """ You can determine the specific hierarchy of the files where will save your data.
        
        """
        self.full_path = self.path
        for elt in liste:
            self.full_path += '/'+elt


class ImportFromPoloniex(ImportDataCryptoCurrencies):
    """ Class to download data from Poloniex exchange.
    
    """
    def __init__(self, path, crypto, span, fiat = 'USDT', form = 'xlsx'):
        """ Initialisation.
        
            path :   The path where data will be save.
            crypto : The abreviation of the crypto-currencies as string.
            span :   As string for 'weekly', 'daily', 'hourly', or the integer 
                     of the seconds between each observations. Min 300 seconds.
            fiat :   Basically the fiat as you want, but can also be an 
                     crypto-currencies. Poloniex don't allow fiat currencies, 
                     but USD theter.
            form :   As string, your favorit format e.g. 'csv', 'txt', 'xlsx'.
                     Only xlsx is allowed for the moment.
            
        """
        if fiat in ['EUR', 'USD']:
            print("Poloniex don't allow fiat currencies, the equivalent of US dollar is Tether USD.")
            self.fiat = fiat = 'USDT'
        if crypto == 'XBT':
            crypto = 'BTC'
        ImportDataCryptoCurrencies.__init__(self, path, crypto, span, 'Poloniex', fiat, form)
        self.pair = self.fiat+'_'+crypto
        self.full_path = self.path+'/Poloniex/Data/Clean_Data/'+str(self.per)+'/'+str(self.crypto)+str(self.fiat)
        
    
    def import_data(self, start = 'last', end = 'now'):
        """ Download data from poloniex for specific time interval
            
            start : Timestamp of the first observation of you want, as integer.
            end :   Timestamp of the last observation of you want as integer.
            
        """
        self.start, self.end = self.time(start, end)
        param = {
            'command' : 'returnChartData', 
            'currencyPair' : self.pair, 
            'start':self.start, 
            'end': self.end, 
            'period' : self.span
        }
        r = requests.get('https://poloniex.com/public', param)
        return self.sort_data(json.loads(r.text))


class ImportFromKraken(ImportDataCryptoCurrencies):
    """ Class to download data from Kraken exchange.
    
    """
    def __init__(self, path, crypto, span, fiat = 'USD', form = 'xlsx'):
        """ Initialisation.
        
            path :   The path where data will be save.
            crypto : The abreviation of the crypto-currencies as string.
            span :   As string for 'weekly', 'daily', 'hourly', or the integer 
                     of the seconds between each observations. Min 60 seconds.
            fiat :   Basically the fiat as you want, but can also be an 
                     crypto-currencies.
            form :   As string, your favorit format e.g. 'csv', 'txt', 'xlsx'.
            
        """
        ImportDataCryptoCurrencies.__init__(self, path, crypto, span, 'Kraken', fiat, form)
        if crypto == 'BTC':
            crypto = 'XBT'
        if crypto == 'BCH' or crypto == 'DASH':
            self.pair = crypto+fiat
        elif fiat not in ['EUR', 'USD', 'CAD', 'JPY', 'GBP']:
            self.pair = 'X'+crypto+'X'+fiat
        else:
            self.pair = 'X'+crypto+'Z'+fiat
        
    
    def import_data(self, start = 'last'):
        """ Download data from Kraken since a specific time until now
        
            start : Timestamp of the first observation of you want, as integer.
            
        """
        self.start, self.end = self.time(start, time.time())
        param = {
            'pair' : self.pair, 
            'interval' : int(self.span/60), 
            'since' : self.start-self.span
        }
        r = requests.get('https://api.kraken.com/0/public/OHLC', param)
        text = json.loads(r.text)['result'][self.pair]
        data = [{'date' : float(e[0]), 'open' : float(e[1]), 'high' : float(e[2]), 
                 'low' : float(e[3]), 'close' : float(e[4]), 
                 'weightedAverage' : float(e[5]), 'volume': float(e[6]), 
                 'quoteVolume':float(e[6])*float(e[5])} for e in text]
        return self.sort_data(data)