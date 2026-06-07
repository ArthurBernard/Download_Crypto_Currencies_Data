"""Default-lookback safety: a first 'last' backfill must be bounded per type.

Regression for the footgun where `start="last"` on an empty dataset fell back to
30 days for *every* type — harmless for OHLC (~720 1h bars) but millions of rows
for trades.
"""

import pytest

from dccd.application.jobs import JobParams, JobSpec, JobTarget, Trigger
from dccd.application.operations import _DEFAULT_LOOKBACK_NS, backfill
from dccd.domain.capability import Capability
from dccd.domain.symbol import Symbol
from dccd.domain.timeutils import NS
from dccd.domain.types import DataType
from dccd.sources.base import OHLCHistory, TradesHistory
from dccd.storage.parquet import ParquetStore


class _FakeSource(OHLCHistory, TradesHistory):
    exchange = "fake"

    def __init__(self) -> None:
        self.calls: list = []

    def capabilities(self) -> list[Capability]:
        return [
            Capability(data_type=DataType.OHLC, transport="rest", mode="historical",
                       history="full", max_per_request=1000, page_direction="forward"),
            Capability(data_type=DataType.TRADES, transport="rest", mode="historical",
                       history="full", max_per_request=1000, page_direction="forward"),
        ]

    async def fetch_ohlc_page(self, symbol, span, start_ns, end_ns, limit):
        self.calls.append(("ohlc", start_ns, end_ns))
        return []

    async def fetch_trades_page(self, symbol, start_ns, end_ns, limit, cursor=None):
        self.calls.append(("trades", start_ns, end_ns))
        return [], None


class _FakeReg:
    def __init__(self, src): self._s = src
    def get(self, ex): return self._s


def test_lookback_table_bounds_trades():
    assert _DEFAULT_LOOKBACK_NS[DataType.TRADES] <= 86400 * NS  # ≤ 1 day
    assert _DEFAULT_LOOKBACK_NS[DataType.OHLC] >= 7 * 86400 * NS


@pytest.mark.asyncio
async def test_first_trades_backfill_window_is_short(tmp_path):
    src = _FakeSource()
    store = ParquetStore(tmp_path)
    target = JobTarget(exchange="fake", symbol=Symbol(base="BTC", quote="USDT"),
                       data_type=DataType.TRADES, span=None)
    spec = JobSpec(id="x", operation="backfill", target=target,
                   trigger=Trigger(kind="once"), params=JobParams(start="last"))
    await backfill(spec, registry=_FakeReg(src), store=store)
    assert src.calls, "adapter was never called"
    _, s, e = src.calls[0]
    window_h = (e - s) / NS / 3600
    # 1 hour default — not 30 days (720h).
    assert window_h <= 2, f"trades first-backfill window too wide: {window_h:.0f}h"
