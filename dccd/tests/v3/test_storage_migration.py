"""WS-B — legacy v2 → v3 schema migration, defensive merge, provenance."""

import polars as pl

from dccd.domain.dataset import DatasetId, Provenance
from dccd.domain.records import OHLCBar
from dccd.domain.symbol import Symbol
from dccd.domain.timeutils import NS
from dccd.domain.types import DataType
from dccd.storage.migrate import migrate_parquet_to_ns, needs_migration
from dccd.storage.parquet import ParquetStore, canonicalize


def _ds():
    return DatasetId(
        exchange="okx", symbol=Symbol(base="BTC", quote="USDT"),
        data_type=DataType.OHLC, span=60,
    )


def _legacy_v2_ohlc(ts_values: list[int], ns: bool) -> pl.DataFrame:
    """A v2-schema OHLC frame (quoteVolume + weightedAverage, no `trades`)."""
    scale = NS if ns else 1
    return pl.DataFrame({
        "TS": [t * scale for t in ts_values],
        "open": [1.0] * len(ts_values),
        "high": [2.0] * len(ts_values),
        "low": [0.5] * len(ts_values),
        "close": [1.5] * len(ts_values),
        "volume": [10.0] * len(ts_values),
        "quoteVolume": [15.0] * len(ts_values),
        "weightedAverage": [1.4] * len(ts_values),
    })


class TestCanonicalize:
    def test_renames_and_drops_legacy_columns(self):
        out = canonicalize(_legacy_v2_ohlc([1, 2, 3], ns=True), DataType.OHLC)
        assert out.columns == ["TS", "open", "high", "low", "close",
                               "volume", "quote_volume", "trades"]
        assert out["quote_volume"].to_list() == [15.0, 15.0, 15.0]
        assert out["trades"].null_count() == 3  # no v2 equivalent → null

    def test_idempotent_on_v3(self):
        bars = [OHLCBar(ts=i * 60 * NS, open=1, high=2, low=0.5, close=1.5, volume=1)
                for i in range(3)]
        store = ParquetStore("/tmp/_x")
        v3 = store._to_dataframe(_ds(), bars)
        assert canonicalize(v3, DataType.OHLC).columns == v3.columns


class TestMigration:
    def test_full_migration_seconds_v2_to_v3(self, tmp_path):
        # A real-world legacy file: v2 columns AND second-scale timestamps.
        f = tmp_path / "okx" / "ohlc" / "BTC-USDT" / "1m" / "2024.parquet"
        f.parent.mkdir(parents=True)
        _legacy_v2_ohlc([1_700_000_000, 1_700_000_060], ns=False).write_parquet(f)

        assert needs_migration(f)
        report = migrate_parquet_to_ns(tmp_path, dry_run=False)
        assert report[0]["rescaled"] is True
        assert report[0]["to_schema"][-2:] == ["quote_volume", "trades"]

        df = pl.read_parquet(f)
        assert df.columns[-2:] == ["quote_volume", "trades"]
        assert int(df["TS"].min()) > 1_000_000_000_000_000_000  # ns now
        assert len(df) == 2  # zero row loss
        assert not needs_migration(f)  # idempotent

    def test_half_migrated_file_ns_but_v2_columns(self, tmp_path):
        # The exact stuck state found on disk: ns timestamps, v2 columns.
        f = tmp_path / "okx" / "ohlc" / "BTC-USDT" / "1m" / "2025.parquet"
        f.parent.mkdir(parents=True)
        _legacy_v2_ohlc([1_735_689_600, 1_735_689_660], ns=True).write_parquet(f)

        assert needs_migration(f)  # columns differ even though TS already ns
        migrate_parquet_to_ns(tmp_path, dry_run=False)
        df = pl.read_parquet(f)
        assert "quote_volume" in df.columns and "weightedAverage" not in df.columns
        assert int(df["TS"].min()) == 1_735_689_600 * NS  # NOT rescaled twice

    def test_dry_run_changes_nothing(self, tmp_path):
        f = tmp_path / "okx" / "ohlc" / "BTC-USDT" / "1m" / "2024.parquet"
        f.parent.mkdir(parents=True)
        _legacy_v2_ohlc([1_700_000_000], ns=False).write_parquet(f)
        before = f.read_bytes()
        report = migrate_parquet_to_ns(tmp_path, dry_run=True)
        assert report[0]["migrated"] is False
        assert f.read_bytes() == before


class TestDefensiveMerge:
    def test_backfill_onto_legacy_v2_file_no_loss(self, tmp_path):
        """Writing v3 bars into an existing v2 file must not drop v2 rows."""
        store = ParquetStore(tmp_path)
        ds = _ds()
        # Seed a legacy v2 file at the canonical path for span=60 / year 2024.
        d = store.directory(ds)
        _legacy_v2_ohlc([1_704_067_200, 1_704_067_260], ns=True).write_parquet(
            d / "2024.parquet"
        )

        # New v3 bar in the same year — triggers merge with the legacy file.
        new_bar = OHLCBar(ts=1_704_067_320 * NS, open=9, high=9, low=9,
                          close=9, volume=1, quote_volume=9.0, trades=3)
        store.save(ds, [new_bar], Provenance(source="okx:rest"))

        df = pl.read_parquet(d / "2024.parquet")
        assert len(df) == 3  # 2 legacy + 1 new, none lost
        assert df.columns[-2:] == ["quote_volume", "trades"]


class TestProvenance:
    def test_provenance_round_trip(self, tmp_path):
        store = ParquetStore(tmp_path)
        ds = _ds()
        bar = OHLCBar(ts=1_704_067_200 * NS, open=1, high=2, low=0.5,
                      close=1.5, volume=1, quote_volume=1.0, trades=1)
        store.save(ds, [bar], Provenance(source="okx:rest"))
        f = next((store.directory(ds)).glob("*.parquet"))
        prov = ParquetStore.read_provenance(f)
        assert prov is not None and prov.source == "okx:rest"
