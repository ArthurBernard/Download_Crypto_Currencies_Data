"""Tests for application layer — config, events, jobs, registry."""

import pytest

from dccd.application.config import AppConfig, JobConfig
from dccd.application.events import EventBus, LogEvent, ProgressEvent, StatusEvent
from dccd.application.jobs import JobParams, JobSpec, JobTarget, RunState, Trigger
from dccd.application.operations import backfill
from dccd.application.registry import REGISTRY
from dccd.domain.capability import Capability
from dccd.domain.symbol import Symbol
from dccd.domain.types import DataType
from dccd.sources.base import OHLCHistory
from dccd.storage.parquet import ParquetStore

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

class TestAppConfig:
    def test_default_config(self):
        cfg = AppConfig()
        assert cfg.settings.data_path == "./data/crypto"
        assert cfg.settings.timezone == "local"

    def test_propagate_data_path(self):
        cfg = AppConfig()
        assert cfg.storage.local_path == "./data/crypto"

    def test_no_jobs_ok(self):
        cfg = AppConfig()
        assert cfg.jobs == []

    def test_all_job_specs_empty(self):
        cfg = AppConfig()
        assert cfg.all_job_specs() == []

    def test_job_config_expands_pairs(self):
        jc = JobConfig(
            exchange="binance",
            pairs=["BTC/USDT", "ETH/USDT"],
            data_type="ohlc",
            span=3600,
            trigger_kind="interval",
            every=3600,
        )
        specs = jc.to_job_specs()
        assert len(specs) == 2
        assert specs[0].target.symbol == Symbol(base="BTC", quote="USDT")
        assert specs[1].target.symbol == Symbol(base="ETH", quote="USDT")

    def test_job_spec_id_format(self):
        jc = JobConfig(
            exchange="binance",
            pairs=["BTC/USDT"],
            data_type="ohlc",
            span=3600,
            trigger_kind="interval",
            every=3600,
        )
        specs = jc.to_job_specs()
        assert "binance" in specs[0].id
        assert "ohlc" in specs[0].id


# ---------------------------------------------------------------------------
# EventBus
# ---------------------------------------------------------------------------

class TestEventBus:
    def test_emit_progress(self):
        bus = EventBus()
        received = []
        bus.subscribe(received.append)
        bus.emit(ProgressEvent(run_id="r1", done=5, total=10))
        assert len(received) == 1
        assert received[0].done == 5

    def test_emit_log(self):
        bus = EventBus()
        received = []
        bus.subscribe(received.append)
        bus.emit(LogEvent(run_id="r1", message="hello"))
        assert received[0].message == "hello"

    def test_emit_status(self):
        bus = EventBus()
        received = []
        bus.subscribe(received.append)
        bus.emit(StatusEvent(run_id="r1", state="succeeded"))
        assert received[0].state == "succeeded"

    def test_unsubscribe(self):
        bus = EventBus()
        received = []
        bus.subscribe(received.append)
        bus.unsubscribe(received.append)
        bus.emit(LogEvent(run_id="r1", message="hello"))
        assert len(received) == 0

    def test_for_run_progress(self):
        bus = EventBus()
        received = []
        bus.subscribe(received.append)
        run_events = bus.for_run("run42")
        run_events.progress(3, 10)
        assert received[0].run_id == "run42"
        assert received[0].done == 3

    def test_for_run_log(self):
        bus = EventBus()
        received = []
        bus.subscribe(received.append)
        run_events = bus.for_run("run42")
        run_events.log("starting backfill")
        assert received[0].message == "starting backfill"

    def test_multiple_handlers(self):
        bus = EventBus()
        a, b = [], []
        bus.subscribe(a.append)
        bus.subscribe(b.append)
        bus.emit(LogEvent(run_id="r1", message="test"))
        assert len(a) == 1
        assert len(b) == 1

    def test_multiple_queues_fan_out(self):
        # Two SSE consumers (e.g. Live + Logs tabs) must each get every event.
        bus = EventBus()
        q1 = bus.add_queue()
        q2 = bus.add_queue()
        bus.emit(LogEvent(run_id="r1", message="hi"))
        assert q1.get_nowait().message == "hi"
        assert q2.get_nowait().message == "hi"

    def test_remove_queue_stops_delivery(self):
        bus = EventBus()
        q = bus.add_queue()
        bus.remove_queue(q)
        bus.emit(LogEvent(run_id="r1", message="hi"))
        assert q.empty()

    def test_sample_event(self):
        from dccd.application.events import StreamSampleEvent
        bus = EventBus()
        received = []
        bus.subscribe(received.append)
        run = bus.for_run("stream:binance:BTC/USDT:trades@stream")
        run.sample(123, value=42153.7)
        assert isinstance(received[0], StreamSampleEvent)
        assert received[0].value == 42153.7
        assert received[0].bid is None and received[0].ask is None
        assert received[0].kind == "sample"
        # order-book sample carries bid/ask instead of value
        run.sample(456, bid=42150.0, ask=42151.0)
        assert received[1].value is None
        assert received[1].bid == 42150.0 and received[1].ask == 42151.0


# ---------------------------------------------------------------------------
# Config — runtime job CRUD (used by the UI)
# ---------------------------------------------------------------------------

class TestJobCrud:
    def test_add_job_returns_id_and_appears(self):
        cfg = AppConfig()
        jid = cfg.add_job(operation="backfill", exchange="binance",
                          pair="BTC/USDT", data_type="ohlc", span=3600,
                          start="2024-01-01")
        assert jid == "backfill:binance:BTC/USDT:ohlc:3600s"
        assert any(s.id == jid for s in cfg.all_job_specs())

    def test_add_duplicate_raises(self):
        cfg = AppConfig()
        cfg.add_job(operation="backfill", exchange="binance", pair="BTC/USDT",
                    data_type="ohlc", span=3600)
        with pytest.raises(ValueError):
            cfg.add_job(operation="backfill", exchange="binance", pair="BTC/USDT",
                        data_type="ohlc", span=3600)

    def test_remove_job(self):
        cfg = AppConfig()
        jid = cfg.add_job(operation="backfill", exchange="binance", pair="BTC/USDT",
                          data_type="ohlc", span=3600)
        assert cfg.remove_job(jid) is True
        assert cfg.all_job_specs() == []
        assert cfg.remove_job(jid) is False

    def test_remove_one_pair_keeps_others(self):
        # A multi-pair JobConfig (as found in a hand-written config.yml).
        cfg = AppConfig(jobs=[JobConfig(exchange="binance",
                        pairs=["BTC/USDT", "ETH/USDT"], data_type="ohlc", span=3600)])
        cfg.remove_job("backfill:binance:BTC/USDT:ohlc:3600s")
        remaining = [s.id for s in cfg.all_job_specs()]
        assert remaining == ["backfill:binance:ETH/USDT:ohlc:3600s"]

    def test_update_job_start_splits_multipair(self):
        cfg = AppConfig(jobs=[JobConfig(exchange="binance",
                        pairs=["BTC/USDT", "ETH/USDT"], data_type="ohlc", span=3600,
                        start="last")])
        assert cfg.update_job_start("backfill:binance:BTC/USDT:ohlc:3600s", "2021-01-01")
        starts = {jc.pairs[0]: jc.start for jc in cfg.jobs}
        assert starts["BTC/USDT"] == "2021-01-01"
        assert starts["ETH/USDT"] == "last"

    def test_update_unknown_returns_false(self):
        cfg = AppConfig()
        assert cfg.update_job_start("nope", "2021-01-01") is False

    def test_update_job_schedule_sets_interval(self):
        cfg = AppConfig()
        jid = cfg.add_job(operation="backfill", exchange="binance",
                          pair="BTC/USDT", data_type="ohlc", span=60,
                          trigger_kind="manual")
        assert cfg.update_job_schedule(jid, 3600) is True
        spec = next(s for s in cfg.all_job_specs() if s.id == jid)
        assert spec.trigger.kind == "interval"
        assert spec.trigger.every == 3600

    def test_update_job_schedule_clear_to_manual(self):
        cfg = AppConfig()
        jid = cfg.add_job(operation="backfill", exchange="binance",
                          pair="BTC/USDT", data_type="ohlc", span=60,
                          trigger_kind="interval", every=3600)
        assert cfg.update_job_schedule(jid, None) is True
        spec = next(s for s in cfg.all_job_specs() if s.id == jid)
        assert spec.trigger.kind == "manual"
        assert spec.trigger.every is None

    def test_update_job_schedule_rejects_below_span(self):
        cfg = AppConfig()
        jid = cfg.add_job(operation="backfill", exchange="binance",
                          pair="BTC/USDT", data_type="ohlc", span=3600,
                          trigger_kind="manual")
        with pytest.raises(ValueError):
            cfg.update_job_schedule(jid, 60)


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------

class TestJobs:
    def test_make_id(self):
        target = JobTarget(
            exchange="binance",
            symbol=Symbol(base="BTC", quote="USDT"),
            data_type=DataType.OHLC,
            span=3600,
        )
        id_ = JobSpec.make_id("backfill", target)
        assert "backfill" in id_
        assert "binance" in id_
        assert "ohlc" in id_

    def test_trigger_once(self):
        t = Trigger(kind="once")
        assert t.kind == "once"

    def test_trigger_interval(self):
        t = Trigger(kind="interval", every=3600)
        assert t.every == 3600

    def test_job_params_default(self):
        p = JobParams()
        assert p.start == "last"

    def test_job_params_accepts_iso_date(self):
        # Regression for D6: custom ISO start date must not be rejected.
        assert JobParams(start="2024-01-01").start == "2024-01-01"

    def test_run_state_enum(self):
        assert RunState.RUNNING == "running"
        assert RunState.SUCCEEDED == "succeeded"


# ---------------------------------------------------------------------------
# Backfill — market capability honesty
# ---------------------------------------------------------------------------

class _MarketFakeSource(OHLCHistory):
    """Minimal OHLC-only adapter whose declared markets are injectable."""

    exchange = "fake"

    def __init__(self, markets: list[str] | None) -> None:
        self._markets = markets
        self.fetch_calls = 0

    def capabilities(self) -> list[Capability]:
        return [
            Capability(
                data_type=DataType.OHLC, transport="rest", mode="historical",
                history="full", max_per_request=1000, page_direction="forward",
                markets=self._markets,
            ),
        ]

    async def fetch_ohlc_page(self, symbol, span, start_ns, end_ns, limit):
        self.fetch_calls += 1
        return []


class _MarketFakeReg:
    def __init__(self, src):
        self._s = src

    def get(self, ex):
        return self._s


class TestBackfillMarketCapability:
    """A perp target must be rejected before any fetch unless the adapter's
    capability declares it — the capability-honesty invariant extended to
    non-spot markets."""

    def _spec(self, market: str) -> JobSpec:
        target = JobTarget(
            exchange="fake",
            symbol=Symbol(base="BTC", quote="USDT", market=market),
            data_type=DataType.OHLC,
            span=3600,
        )
        return JobSpec(
            id="x", operation="backfill", target=target,
            trigger=Trigger(kind="once"), params=JobParams(start="last"),
        )

    @pytest.mark.asyncio
    async def test_perp_rejected_when_capability_is_spot_only(self, tmp_path):
        src = _MarketFakeSource(markets=None)
        store = ParquetStore(tmp_path)
        spec = self._spec("perp")
        result = await backfill(spec, registry=_MarketFakeReg(src), store=store)
        assert "error" in result
        assert "perp" in result["error"]
        assert src.fetch_calls == 0

    @pytest.mark.asyncio
    async def test_perp_accepted_when_declared(self, tmp_path):
        src = _MarketFakeSource(markets=["perp"])
        store = ParquetStore(tmp_path)
        spec = self._spec("perp")
        result = await backfill(spec, registry=_MarketFakeReg(src), store=store)
        assert "error" not in result

    @pytest.mark.asyncio
    async def test_spot_still_proceeds_unaffected(self, tmp_path):
        # Regression: spot behaviour must stay untouched by the market check.
        src = _MarketFakeSource(markets=None)
        store = ParquetStore(tmp_path)
        spec = self._spec("spot")
        result = await backfill(spec, registry=_MarketFakeReg(src), store=store)
        assert "error" not in result
        assert src.fetch_calls >= 1


# ---------------------------------------------------------------------------
# Operation Registry
# ---------------------------------------------------------------------------

class TestSchedulerSyncIntervals:
    """Scheduler reconciles recurring backfill loops without a restart."""

    def _spec(self, every):
        target = JobTarget(exchange="binance",
                           symbol=Symbol(base="BTC", quote="USDT"),
                           data_type=DataType.OHLC, span=60)
        return JobSpec(
            id=JobSpec.make_id("backfill", target),
            operation="backfill", target=target,
            trigger=Trigger(kind="interval", every=every),
        )

    @pytest.mark.asyncio
    async def test_sync_intervals_adds_and_removes(self):
        from dccd.application.scheduler import Scheduler

        sched = Scheduler(registry=None, store=None)  # type: ignore[arg-type]

        async def _noop(spec):
            return None
        sched._run_once = _noop  # type: ignore[assignment]

        spec = self._spec(3600)
        # Not running → no-op.
        await sched.sync_intervals([spec])
        assert spec.id not in sched._interval_loops

        sched._running = True
        await sched.sync_intervals([spec])
        assert spec.id in sched._interval_loops

        # Schedule removed → loop cancelled and dropped.
        await sched.sync_intervals([])
        assert spec.id not in sched._interval_loops
        await sched.stop()

    @pytest.mark.asyncio
    async def test_sync_intervals_restarts_on_changed_cadence(self):
        from dccd.application.scheduler import Scheduler

        sched = Scheduler(registry=None, store=None)  # type: ignore[arg-type]

        async def _noop(spec):
            return None
        sched._run_once = _noop  # type: ignore[assignment]
        sched._running = True

        await sched.sync_intervals([self._spec(3600)])
        first = sched._interval_loops[JobSpec.make_id(
            "backfill",
            JobTarget(exchange="binance", symbol=Symbol(base="BTC", quote="USDT"),
                      data_type=DataType.OHLC, span=60))][0]

        await sched.sync_intervals([self._spec(86400)])
        sid = JobSpec.make_id(
            "backfill",
            JobTarget(exchange="binance", symbol=Symbol(base="BTC", quote="USDT"),
                      data_type=DataType.OHLC, span=60))
        assert sched._interval_loops[sid][1] == 86400
        assert sched._interval_loops[sid][0] is not first
        await sched.stop()


class TestOperationRegistry:
    def test_all_operations_registered(self):
        assert "backfill" in REGISTRY.operations
        assert "stream" in REGISTRY.operations
        assert "read" in REGISTRY.operations
        assert "inventory" in REGISTRY.operations

    def test_get_backfill_spec(self):
        spec = REGISTRY.get("backfill")
        assert spec.name == "backfill"
        assert "exchange" in spec.input_schema

    def test_get_unknown(self):
        with pytest.raises(KeyError):
            REGISTRY.get("unknown_op")

    def test_parity_api_cli(self):
        """Each operation must be accessible from both CLI and API (parity test)."""
        required_ops = {"backfill", "stream", "read", "inventory"}
        assert required_ops.issubset(set(REGISTRY.operations))


class TestHealthMonitor:
    """HealthMonitor fires a webhook on N consecutive failures and resets on success."""

    def test_alerts_after_threshold_and_resets(self, monkeypatch):
        import urllib.request

        from dccd.application.events import EventBus, StatusEvent
        from dccd.application.monitor import HealthMonitor

        calls: list[str] = []

        class _Resp:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        def fake_urlopen(req, timeout=0):
            calls.append(getattr(req, "full_url", str(req)))
            return _Resp()

        monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

        bus = EventBus()
        HealthMonitor(None, bus, webhook_url="http://hook.test", max_consecutive_errors=3)

        # Distinct run ids that share one job (spec id) — each backfill run is
        # unique (`{spec}@{ts}`), so the monitor must accumulate across runs.
        job = "backfill:binance:BTC/USDT:ohlc:3600s"
        # Below threshold: no alert.
        bus.emit(StatusEvent(run_id=f"{job}@1", state="failed"))
        bus.emit(StatusEvent(run_id=f"{job}@2", state="failed"))
        assert calls == []

        # Threshold reached (3rd failure, different run): one alert.
        bus.emit(StatusEvent(run_id=f"{job}@3", state="failed"))
        assert len(calls) == 1

        # A success resets the counter; failures must re-accumulate.
        bus.emit(StatusEvent(run_id=f"{job}@4", state="succeeded"))
        bus.emit(StatusEvent(run_id=f"{job}@5", state="failed"))
        bus.emit(StatusEvent(run_id=f"{job}@6", state="failed"))
        assert len(calls) == 1
        bus.emit(StatusEvent(run_id=f"{job}@7", state="failed"))
        assert len(calls) == 2

        # A different job keeps its own independent counter.
        bus.emit(StatusEvent(run_id="backfill:kraken:BTC/USD:ohlc:3600s@1", state="failed"))
        assert len(calls) == 2

    def test_no_webhook_no_crash(self):
        from dccd.application.events import EventBus, StatusEvent
        from dccd.application.monitor import HealthMonitor

        bus = EventBus()
        HealthMonitor(None, bus, webhook_url=None, max_consecutive_errors=1)
        # Must not raise even past threshold when no webhook is configured.
        bus.emit(StatusEvent(run_id="r1", state="failed"))
        bus.emit(StatusEvent(run_id="r1", state="failed"))
