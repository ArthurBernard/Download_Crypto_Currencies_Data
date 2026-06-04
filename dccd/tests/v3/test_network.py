"""End-to-end tests against real exchange APIs (WS-G4).

Excluded from the default run (`-m 'not network'`). Run explicitly with:

    pytest dccd/tests/v3/test_network.py -m network

They validate that cursor-based trades pagination (WS-A) actually drains a
window past a single capped page — the regression that silently dropped >95% of
trades on every exchange but Binance.
"""

import pytest

from dccd.application.jobs import JobParams, JobSpec, JobTarget, Trigger
from dccd.application.operations import backfill
from dccd.application.service_factory import build_registry, build_store
from dccd.domain.dataset import DatasetId
from dccd.domain.symbol import Symbol
from dccd.domain.timeutils import NS, ns_now
from dccd.domain.types import DataType

pytestmark = pytest.mark.network


async def _drain_trades(src, symbol, window_s, max_pages=500):
    """Drain a trades window; return (trades, pages, capability).

    ``pages`` counts the underlying fetch calls — proving the cursor was
    followed past the first capped page (volume-independent, unlike a raw count).
    """
    from dccd.transport.paginate import paginate_trades

    cap = src.capability_for(DataType.TRADES, "rest", "historical")
    end = ns_now() - 5 * 60 * NS  # 5 min ago, to avoid the very latest edge
    start = end - window_s * NS
    pages = 0

    async def fetch(s, e, limit, cursor):
        nonlocal pages
        pages += 1
        return await src.fetch_trades_page(symbol, s, e, limit, cursor)

    out = []
    async for t in paginate_trades(fetch, cap, start, end, max_pages=max_pages):
        out.append(t)
    return out, pages, cap


def _coverage_s(trades):
    ts = [t.ts for t in trades]
    return (max(ts) - min(ts)) / NS if ts else 0.0


@pytest.mark.asyncio
async def test_binance_trades_drains_multiple_pages():
    from dccd.sources.binance import BinanceSource

    window = 900  # 15 min — comfortably exceeds the 1000/page cap on BTC/USDT
    trades, pages, cap = await _drain_trades(
        BinanceSource(), Symbol(base="BTC", quote="USDT"), window)
    assert pages > 1, "cursor not followed past the first page"
    assert len(trades) > cap.max_per_request
    assert _coverage_s(trades) >= 0.8 * window  # old bug covered only page 1
    assert all(t.ts > 0 for t in trades)


@pytest.mark.asyncio
async def test_okx_trades_drains_multiple_pages():
    from dccd.sources.okx import OKXSource

    # OKX caps at 100/page and was fully broken before WS-A.
    window = 300
    trades, pages, cap = await _drain_trades(
        OKXSource(), Symbol(base="BTC", quote="USDT"), window)
    assert pages > 1
    assert len(trades) > cap.max_per_request
    assert _coverage_s(trades) >= 0.8 * window


@pytest.mark.asyncio
async def test_binance_ohlc_backfill_roundtrip(tmp_path):
    store = build_store(tmp_path)
    reg = build_registry()
    target = JobTarget(exchange="binance", symbol=Symbol(base="BTC", quote="USDT"),
                       data_type=DataType.OHLC, span=3600)
    # Last ~3 days of hourly candles.
    start_ns = ns_now() - 3 * 86400 * NS
    spec = JobSpec(id=JobSpec.make_id("backfill", target), operation="backfill",
                   target=target, trigger=Trigger(kind="once"),
                   params=JobParams(start=str(start_ns)), origin="runtime")
    result = await backfill(spec, registry=reg, store=store)

    assert "error" not in result
    assert result["rows_written"] > 48  # ~72 hourly bars over 3 days
    ds = DatasetId(exchange="binance", symbol=Symbol(base="BTC", quote="USDT"),
                   data_type=DataType.OHLC, span=3600)
    df = store.load(ds)
    ts = df["TS"].to_list()
    assert ts == sorted(ts)  # monotonic, deduplicated
    assert df["quote_volume"].null_count() == 0  # Binance provides it natively
