import time

import pandas as pd

import pytest

from dccd import FromGDax as fg

@pytest.fixture
def init_loader():
    return fg('/home/arthur/Data/Crypto_Currencies/', 'XBT', 86400, 'USD')

def test_import_data(init_loader):
    start = time.time() // 86400 * 86400 - 86400
    data = init_loader._import_data(start=start)
    list_keys = ['date', 'open', 'high', 'low', 'close', 'volume', 'quoteVolume']
    assert isinstance(data, list)
    assert isinstance(data[0], dict)
    for key in list_keys:
        assert key in data[0].keys()
    # assert isinstance(data[0]['date'], int) # /! date is float
    # assert data[0]['date'] == start # /! date is float