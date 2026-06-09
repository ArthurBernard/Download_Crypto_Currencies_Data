"""Coverage manifest: remember a dataset's extent so dropped local data isn't
re-downloaded on the next ``backfill(start="last")``.
"""

from __future__ import annotations

from dccd.application.jobs import JobParams, JobSpec, JobTarget, Trigger
from dccd.application.operations import backfill
from dccd.domain.capability import Capability
from dccd.domain.dataset import DatasetId
from dccd.domain.records import OHLCBar
from dccd.domain.symbol import Symbol
from dccd.domain.timeutils import NS, ns_now
from dccd.domain.types import DataType
from dccd.sources.base import OHLCHistory
from dccd.storage.coverage_sqlite import CoverageStore
from dccd.storage.parquet import ParquetStore

_SPAN = 3600
_SYMBOL = Symbol(base="BTC", quote="USDT")


def _ds() -> DatasetId:
    return DatasetId(exchange="fake", symbol=_SYMBOL, data_type=DataType.OHLC, span=_SPAN)


class _FakeOHLC(OHLCHistory):
    """OHLC adapter that records the start_ns it is asked for and optionally
    serves a fixed set of bars on the first page."""

    exchange = "fake"

    def __init__(self, bars: list[OHLCBar] | None = None) -> None:
        self.calls: list[int] = []
        self._bars = bars or []
        self._served = False

    def capabilities(self) -> list[Capability]:
        return [
            Capability(data_type=DataType.OHLC, transport="rest", mode="historical",
                       history="full", max_per_request=1000, page_direction="forward"),
        ]

    async def fetch_ohlc_page(self, symbol, span, start_ns, end_ns, limit):
        self.calls.append(start_ns)
        if self._served:
            return []
        self._served = True
        return [b for b in self._bars if start_ns <= b.ts <= end_ns]


class _FakeReg:
    def __init__(self, src): self._s = src
    def get(self, ex): return self._s


# ---------------------------------------------------------------------------
# CoverageStore unit
# ---------------------------------------------------------------------------

class TestCoverageStore:
    def test_record_and_get_max(self, tmp_path):
        cov = CoverageStore(tmp_path / "coverage.db")
        ds = _ds()
        assert cov.get_max_ts(ds) is None
        cov.record(ds, min_ts=100, max_ts=500, rows_added=5)
        assert cov.get_max_ts(ds) == 500

    def test_envelope_widens_never_narrows(self, tmp_path):
        cov = CoverageStore(tmp_path / "coverage.db")
        ds = _ds()
        cov.record(ds, min_ts=100, max_ts=500, rows_added=5)
        # A later run with a narrower window must not shrink the recorded extent.
        cov.record(ds, min_ts=300, max_ts=400, rows_added=2)
        rows = cov.list_all()
        assert len(rows) == 1
        assert rows[0]["min_ts"] == 100
        assert rows[0]["max_ts"] == 500
        assert rows[0]["rows"] == 7  # accumulated
        # A genuinely later max extends it.
        cov.record(ds, min_ts=600, max_ts=900, rows_added=1)
        assert cov.get_max_ts(ds) == 900

    def test_none_timestamps_ignored(self, tmp_path):
        cov = CoverageStore(tmp_path / "coverage.db")
        ds = _ds()
        cov.record(ds, min_ts=None, max_ts=None, rows_added=0)
        assert cov.get_max_ts(ds) is None


# ---------------------------------------------------------------------------
# backfill integration — resume + record
# ---------------------------------------------------------------------------

class TestBackfillCoverage:
    async def test_resumes_from_manifest_when_local_empty(self, tmp_path):
        """Local files dropped → start='last' resumes from the manifest's max_ts."""
        cov = CoverageStore(tmp_path / ".dccd" / "coverage.db")
        ds = _ds()
        # Span-aligned, ~10 bars ago (OHLC start snaps to the bar boundary).
        recorded_max = (ns_now() // (_SPAN * NS) - 10) * (_SPAN * NS)
        cov.record(ds, min_ts=recorded_max - 100 * _SPAN * NS, max_ts=recorded_max)

        src = _FakeOHLC()
        store = ParquetStore(tmp_path)  # empty → last_timestamp is None
        target = JobTarget(exchange="fake", symbol=_SYMBOL,
                           data_type=DataType.OHLC, span=_SPAN)
        spec = JobSpec(id="x", operation="backfill", target=target,
                       trigger=Trigger(kind="once"), params=JobParams(start="last"))
        await backfill(spec, registry=_FakeReg(src), store=store, coverage_store=cov)

        assert src.calls, "adapter never called"
        # Resumed from the manifest (within a bar of recorded_max), NOT the
        # 30-day bounded default an empty store would otherwise use.
        assert abs(src.calls[0] - recorded_max) <= _SPAN * NS
        default_start = ns_now() - 30 * 86400 * NS
        assert src.calls[0] > default_start + 86400 * NS

    async def test_records_extent_on_success(self, tmp_path):
        cov = CoverageStore(tmp_path / ".dccd" / "coverage.db")
        ds = _ds()
        base = ns_now() - 5 * _SPAN * NS
        bars = [
            OHLCBar(ts=base + i * _SPAN * NS, open=1.0, high=2.0, low=0.5,
                    close=1.5, volume=10.0)
            for i in range(3)
        ]
        src = _FakeOHLC(bars=bars)
        store = ParquetStore(tmp_path)
        target = JobTarget(exchange="fake", symbol=_SYMBOL,
                           data_type=DataType.OHLC, span=_SPAN)
        # Explicit ns start just before the bars so they fall in the first page.
        spec = JobSpec(id="x", operation="backfill", target=target,
                       trigger=Trigger(kind="once"),
                       params=JobParams(start=str(base - _SPAN * NS)))
        await backfill(spec, registry=_FakeReg(src), store=store, coverage_store=cov)

        assert cov.get_max_ts(ds) == bars[-1].ts
        rows = cov.list_all()
        assert rows and rows[0]["min_ts"] == bars[0].ts
