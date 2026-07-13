"""Tests for the CryptoHFTData multi-venue historical provider."""

from __future__ import annotations

from collections import namedtuple
from datetime import datetime, timezone

import pytest

from dccd.application.config import SUPPORTED_EXCHANGES, JobConfig
from dccd.application.jobs import JobParams, JobSpec, JobTarget, Trigger
from dccd.application.operations import backfill
from dccd.application.service_factory import build_registry, build_store
from dccd.domain.dataset import DatasetId
from dccd.domain.symbol import Symbol
from dccd.domain.timeutils import NS
from dccd.domain.types import DataType
from dccd.sources.cryptohftdata import CryptoHFTDataSource
from dccd.sources.registry import SourceRegistry

_Row = namedtuple(
    "Row",
    "trade_time trade_id price quantity is_buyer_maker",
)


class _Frame:
    """Small pandas-compatible fixture used without adding pandas to dev deps."""

    columns = ["trade_time", "trade_id", "price", "quantity", "is_buyer_maker"]

    def __init__(self, rows):
        self._rows = list(rows)

    @property
    def empty(self):
        return not self._rows

    def sort_values(self, columns, kind):
        assert columns == ["trade_time", "trade_id"]
        assert kind == "stable"
        return _Frame(sorted(self._rows, key=lambda row: (row.trade_time, row.trade_id)))

    def itertuples(self, index=False):
        assert index is False
        return iter(self._rows)


class _Client:
    def __init__(self, rows_by_date):
        self.rows_by_date = rows_by_date
        self.calls = []

    def get_trades(self, **kwargs):
        self.calls.append(kwargs)
        return _Frame(self.rows_by_date.get(kwargs["start_date"], []))


def _ms(value: datetime) -> int:
    return int(value.timestamp() * 1_000)


def _source(client):
    return CryptoHFTDataSource(
        exchange="cryptohftdata-binance-futures",
        venue="binance_futures",
        markets=["perp"],
        client=client,
    )


@pytest.mark.asyncio
async def test_provider_maps_and_cursor_pages_sdk_trades():
    """SDK rows become canonical ns trades without exceeding page limits."""
    start = datetime(2026, 7, 11, tzinfo=timezone.utc)
    rows = [
        _Row(_ms(start), 3, "9", "4", False),
        _Row(_ms(start), 1, "10", "2", True),
        _Row(_ms(start.replace(second=1)), 4, "11", "5", False),
    ]
    client = _Client({"2026-07-11": rows})
    source = _source(client)
    start_ns = int(start.timestamp() * NS)

    first, cursor = await source.fetch_trades_page(
        Symbol(base="KAVA", quote="USDT", market="perp"),
        start_ns,
        start_ns + 2 * NS,
        limit=2,
    )
    second, final_cursor = await source.fetch_trades_page(
        Symbol(base="KAVA", quote="USDT", market="perp"),
        start_ns,
        start_ns + 2 * NS,
        limit=2,
        cursor=cursor,
    )

    assert len(first) == 2
    assert len(second) == 1
    assert final_cursor is None
    assert [trade.tid for trade in first + second] == ["1", "3", "4"]
    assert first[0].side == "sell"
    assert first[0].ts == start_ns
    assert client.calls[0] == {
        "symbol": "KAVAUSDT",
        "exchange": "binance_futures",
        "start_date": "2026-07-11",
        "end_date": "2026-07-11",
    }


def test_provider_is_registered_and_configurable():
    """The provider-qualified venues are available through normal dccd wiring."""
    exchange = "cryptohftdata-binance-futures"

    assert exchange in SUPPORTED_EXCHANGES
    assert build_registry().get(exchange).venue == "binance_futures"
    job = JobConfig(
        exchange=exchange,
        pairs=["KAVA/USDT:perp"],
        data_type="trades",
        trigger_kind="manual",
    )
    assert job.exchange == exchange


@pytest.mark.asyncio
async def test_provider_backfill_roundtrip_writes_canonical_parquet(tmp_path):
    """The complete adapter → operation → ParquetStore path preserves trades."""
    start = datetime(2026, 7, 11, tzinfo=timezone.utc)
    rows = [
        _Row(_ms(start), 1, "10", "2", True),
        _Row(_ms(start.replace(second=1)), 2, "11", "3", False),
    ]
    source = _source(_Client({"2026-07-11": rows}))
    registry = SourceRegistry()
    registry.register(source.exchange, source)
    store = build_store(tmp_path)
    symbol = Symbol(base="KAVA", quote="USDT", market="perp")
    target = JobTarget(exchange=source.exchange, symbol=symbol, data_type=DataType.TRADES)
    spec = JobSpec(
        id=JobSpec.make_id("backfill", target),
        operation="backfill",
        target=target,
        trigger=Trigger(kind="once"),
        params=JobParams(start="2026-07-11"),
        origin="runtime",
    )

    result = await backfill(spec, registry=registry, store=store)
    dataset = DatasetId(exchange=source.exchange, symbol=symbol, data_type=DataType.TRADES)
    stored = store.load(dataset)

    assert result["rows_written"] == 2
    assert stored.height == 2
    assert stored["TS"].to_list() == [int(start.timestamp() * NS), int(start.timestamp() * NS) + NS]
    assert stored["side"].to_list() == ["sell", "buy"]
