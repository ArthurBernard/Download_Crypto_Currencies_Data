"""A live stream that ends on its own is `failed`, not `cancelled`.

Regression for the mislabel where `stream()` finished every run as `cancelled`
— including WS generators that ended unexpectedly with no stop requested — so
Logs/Runs claimed someone stopped a stream nobody touched.
"""

import asyncio

import pytest

from dccd.application.jobs import JobParams, JobSpec, JobTarget, Trigger
from dccd.application.operations import stream
from dccd.domain.capability import Capability
from dccd.domain.records import Trade
from dccd.domain.symbol import Symbol
from dccd.domain.timeutils import ns_now
from dccd.domain.types import DataType
from dccd.sources.base import TradesLive
from dccd.storage.parquet import ParquetStore
from dccd.storage.runs_sqlite import RunsStore


class _FiniteTradesWS(TradesLive):
    """A trades 'stream' whose generator ends after N records (no stop asked)."""

    exchange = "fake"

    def __init__(self, n: int = 3, forever: bool = False) -> None:
        self._n = n
        self._forever = forever

    def capabilities(self) -> list[Capability]:
        return [Capability(data_type=DataType.TRADES, transport="ws", mode="live")]

    async def stream_trades(self, symbol):
        for i in range(self._n):
            yield Trade(ts=ns_now(), price=1.0 + i, amount=1.0, side="buy", tid=str(i))
        while self._forever:
            await asyncio.sleep(0.01)
            yield Trade(ts=ns_now(), price=9.9, amount=1.0, side="buy", tid="loop")


class _FakeReg:
    def __init__(self, src):
        self._s = src

    def get(self, ex):
        return self._s


def _spec() -> JobSpec:
    target = JobTarget(exchange="fake", symbol=Symbol.parse("BTC/USDT"),
                       data_type=DataType.TRADES)
    return JobSpec(id="stream:fake:BTC/USDT:trades", operation="stream",
                   target=target, trigger=Trigger(kind="supervised"),
                   params=JobParams(), origin="config")


@pytest.mark.asyncio
async def test_unexpected_end_is_failed(tmp_path):
    store = ParquetStore(str(tmp_path / "data"))
    runs = RunsStore(str(tmp_path / "runs.db"))
    await stream(_spec(), registry=_FakeReg(_FiniteTradesWS(3)), store=store,
                 runs_store=runs)
    run = runs.list_runs(limit=1)[0]
    assert run["state"] == "failed"
    assert "unexpectedly" in (run["error"] or "")


@pytest.mark.asyncio
async def test_requested_stop_is_cancelled(tmp_path):
    store = ParquetStore(str(tmp_path / "data"))
    runs = RunsStore(str(tmp_path / "runs.db"))
    stop = asyncio.Event()

    async def _stop_soon():
        await asyncio.sleep(0.05)
        stop.set()

    asyncio.get_event_loop().create_task(_stop_soon())
    await stream(_spec(), registry=_FakeReg(_FiniteTradesWS(3, forever=True)),
                 store=store, runs_store=runs, stop_event=stop)
    run = runs.list_runs(limit=1)[0]
    assert run["state"] == "cancelled"
    assert not run["error"]
