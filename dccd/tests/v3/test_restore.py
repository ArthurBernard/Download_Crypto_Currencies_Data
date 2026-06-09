"""Read-through restore: a purged dataset is pulled back from the remote on read."""

from __future__ import annotations

from dccd.application.jobs import JobTarget
from dccd.application.operations import read
from dccd.domain.dataset import DatasetId, Provenance
from dccd.domain.records import OHLCBar
from dccd.domain.symbol import Symbol
from dccd.domain.types import DataType
from dccd.storage.parquet import ParquetStore
from dccd.storage.remote import RemoteStorage

_SYM = Symbol(base="BTC", quote="USDT")


def _target() -> JobTarget:
    return JobTarget(exchange="binance", symbol=_SYM, data_type=DataType.OHLC, span=3600)


def _ds() -> DatasetId:
    return DatasetId(exchange="binance", symbol=_SYM, data_type=DataType.OHLC, span=3600)


def _bar(ts: int = 1_700_000_000_000_000_000) -> OHLCBar:
    return OHLCBar(ts=ts, open=1.0, high=2.0, low=0.5, close=1.5, volume=10.0)


class _RestoringRemote:
    """Fake remote whose restore() re-materialises the dataset (simulates rclone)."""

    def __init__(self, store: ParquetStore, ds: DatasetId, bar: OHLCBar):
        self._store, self._ds, self._bar = store, ds, bar
        self.restored = False

    def restore(self, rel_path: str) -> bool:
        self.restored = True
        self._store.save(self._ds, [self._bar], Provenance(source="remote"))
        return True


# ---------------------------------------------------------------------------
# RemoteStorage.restore
# ---------------------------------------------------------------------------

class TestRemoteRestore:
    def test_no_remotes_returns_false(self, tmp_path):
        assert RemoteStorage(tmp_path, []).restore("any/path") is False

    def test_missing_rclone_is_graceful(self, tmp_path):
        # rclone is not a test dependency → FileNotFoundError handled → False.
        rs = RemoteStorage(tmp_path, [{"remote": "dummy:bucket"}])
        assert rs.restore("binance/ohlc/BTC-USDT/3600") is False


# ---------------------------------------------------------------------------
# operations.read read-through
# ---------------------------------------------------------------------------

class TestReadThrough:
    def test_restores_when_local_empty(self, tmp_path):
        store = ParquetStore(tmp_path)
        remote = _RestoringRemote(store, _ds(), _bar())
        df = read(_target(), store=store, remote=remote)
        assert remote.restored is True
        assert len(df) == 1

    def test_no_restore_when_files_present(self, tmp_path):
        store = ParquetStore(tmp_path)
        store.save(_ds(), [_bar()], Provenance(source="local"))
        remote = _RestoringRemote(store, _ds(), _bar())
        df = read(_target(), store=store, remote=remote)
        assert remote.restored is False  # local present → no remote hit
        assert len(df) == 1

    def test_no_remote_is_unchanged(self, tmp_path):
        store = ParquetStore(tmp_path)
        df = read(_target(), store=store, remote=None)
        assert len(df) == 0  # empty in, empty out, no error
