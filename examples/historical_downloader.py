#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" Simple example showing how to use the historical downloader.

Exchange classes (FromBinance, FromKraken, FromCoinbase, FromBybit, FromOKX)
share the same interface: initialise with a path, crypto symbol, and time
span in seconds, then call import_data() → save() → get_data().

start / end accept:
  - a timestamp (int)
  - a date string 'yyyy-mm-dd hh:mm:ss'
  - 'last' (resume from last saved point) / 'now' (current time)

"""

from dccd.histo_dl import FromBinance

# Download hourly BTC/USDT data for 2024
obj = FromBinance('/home/arthur/Data/Crypto_Currencies/', 'BTC', 3600, fiat='USDT')

obj.import_data(start='2024-01-01 00:00:00', end='2024-12-31 00:00:00')
obj.save(form='parquet')

df = obj.get_data()
print(df.head())

# Incremental update (resumes from last saved timestamp)
obj.import_data(start='last', end='now').save(form='parquet')
