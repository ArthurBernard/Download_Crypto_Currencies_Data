"""Tests for application layer — config, events, jobs, registry."""

import pytest

from dccd.application.config import AppConfig, JobConfig
from dccd.application.events import EventBus, LogEvent, ProgressEvent, StatusEvent
from dccd.application.jobs import JobParams, JobSpec, JobTarget, RunState, Trigger
from dccd.application.registry import REGISTRY
from dccd.domain.symbol import Symbol
from dccd.domain.types import DataType

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
# Operation Registry
# ---------------------------------------------------------------------------

class TestOperationRegistry:
    def test_all_operations_registered(self):
        assert "backfill" in REGISTRY.operations
        assert "stream" in REGISTRY.operations
        assert "read" in REGISTRY.operations
        assert "inventory" in REGISTRY.operations
        assert "migrate" in REGISTRY.operations

    def test_get_backfill_spec(self):
        spec = REGISTRY.get("backfill")
        assert spec.name == "backfill"
        assert "exchange" in spec.input_schema

    def test_get_unknown(self):
        with pytest.raises(KeyError):
            REGISTRY.get("unknown_op")

    def test_parity_api_cli(self):
        """Each operation must be accessible from both CLI and API (parity test)."""
        required_ops = {"backfill", "stream", "read", "inventory", "migrate"}
        assert required_ops.issubset(set(REGISTRY.operations))
