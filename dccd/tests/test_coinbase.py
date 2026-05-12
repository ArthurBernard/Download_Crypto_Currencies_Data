import time

import pytest

from dccd import FromCoinbase as fc


@pytest.fixture
def init_loader():
    return fc('/home/arthur/Data/Crypto_Currencies/', 'XBT', 86400, 'USD')

def test_import_data(init_loader):
    start = time.time() // 86400 * 86400 - 86400
    data = init_loader._import_data(start=start)
    list_keys = ['date', 'open', 'high', 'low', 'close', 'volume', 'quoteVolume']
    assert isinstance(data, list)
    assert isinstance(data[0], dict)
    for key in list_keys:
        assert key in data[0].keys()
