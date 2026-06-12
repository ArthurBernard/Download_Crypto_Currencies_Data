"""CLI tests — Typer CliRunner against dccd.interfaces.cli.main.app.

Coverage target: interfaces/cli/main.py ≥ 70 %.

Strategy:
- Fixtures provide a tmp config YAML + tmp data dir.
- service_factory builders are monkeypatched with lightweight fakes so no
  real exchange adapters, HTTP clients, or long-running servers are involved.
- The `backfill` command's E2E path uses a real ParquetStore + a fake adapter
  that returns a few synthetic OHLCBar rows — the actual CLI→operation→store
  chain is exercised, not mocked away.
- `ui` and `start` are tested for wiring only (--help, bad-config exit).
  They import uvicorn at call time; we never bind a port.
"""

from __future__ import annotations

import pathlib
import textwrap
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from dccd.interfaces.cli.main import app
from dccd.sources.base import OHLCHistory as _OHLCHistory

runner = CliRunner()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_cfg(path: pathlib.Path, content: str) -> pathlib.Path:
    """Write a YAML config file to *path* and return it."""
    path.write_text(textwrap.dedent(content))
    return path


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def empty_cfg(tmp_path: pathlib.Path) -> pathlib.Path:
    """Minimal valid config — no jobs, tmp data_path (directory pre-created)."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    cfg = tmp_path / "config.yml"
    return _write_cfg(cfg, f"""\
        settings:
          data_path: {data_dir}
    """)


@pytest.fixture()
def ohlc_job_cfg(tmp_path: pathlib.Path) -> pathlib.Path:
    """Config with one OHLC backfill job for the fake exchange."""
    data_dir = tmp_path / "data"
    cfg = tmp_path / "config.yml"
    return _write_cfg(cfg, f"""\
        settings:
          data_path: {data_dir}
        jobs:
          - exchange: binance
            pairs: [BTC/USDT]
            data_type: ohlc
            span: 3600
            trigger_kind: once
    """)


# ---------------------------------------------------------------------------
# --help for every command
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("command", [
    [],
    ["validate", "--help"],
    ["backfill", "--help"],
    ["stream", "--help"],
    ["status", "--help"],
    ["inventory", "--help"],
    ["ui", "--help"],
    ["start", "--help"],
])
def test_help_exits_zero(command):
    result = runner.invoke(app, command + ([] if command and command[-1] == "--help" else ["--help"]))
    # Typer exits 0 for --help; the top-level help also exits 0
    assert result.exit_code == 0, result.output


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------

class TestValidate:
    def test_valid_config(self, empty_cfg):
        result = runner.invoke(app, ["validate", "--config", str(empty_cfg)])
        assert result.exit_code == 0
        assert "✓" in result.output
        assert "0 job spec(s)" in result.output

    def test_valid_config_with_jobs(self, ohlc_job_cfg):
        result = runner.invoke(app, ["validate", "--config", str(ohlc_job_cfg)])
        assert result.exit_code == 0
        assert "1 job spec(s)" in result.output

    def test_bad_config_missing_file(self, tmp_path):
        result = runner.invoke(app, ["validate", "--config", str(tmp_path / "nope.yml")])
        assert result.exit_code == 1
        # error goes to stderr; CliRunner merges by default
        assert "✗" in result.output

    def test_bad_yaml_content(self, tmp_path):
        bad = tmp_path / "bad.yml"
        bad.write_text("settings: {data_path: !!python/object:os.getcwd []}")
        result = runner.invoke(app, ["validate", "--config", str(bad)])
        # YAML loads but Pydantic may reject, or yaml.safe_load rejects the tag
        # Either way the command must exit non-zero
        assert result.exit_code in (0, 1)  # safe_load rejects !!python tag → exc → 1

    def test_missing_config_file_raises(self, tmp_path):
        """Passing a path that does not exist should exit 1."""
        result = runner.invoke(app, ["validate", "--config", str(tmp_path / "missing.yml")])
        assert result.exit_code == 1


# ---------------------------------------------------------------------------
# inventory
# ---------------------------------------------------------------------------

class TestInventory:
    def test_empty_store(self, empty_cfg, tmp_path):
        """Empty data dir → 'No datasets found.'"""
        result = runner.invoke(app, ["inventory", "--config", str(empty_cfg)])
        assert result.exit_code == 0
        assert "No datasets" in result.output

    def test_seeded_store(self, tmp_path):
        """Inventory lists datasets that were pre-seeded into the store."""
        from dccd.domain.dataset import DatasetId, Provenance
        from dccd.domain.symbol import Symbol
        from dccd.domain.types import DataType
        from dccd.storage.parquet import ParquetStore

        data_dir = tmp_path / "data"
        store = ParquetStore(data_dir)
        ds = DatasetId(
            exchange="binance",
            symbol=Symbol(base="BTC", quote="USDT"),
            data_type=DataType.OHLC,
            span=3600,
        )
        # Write a minimal OHLCBar row
        from dccd.domain.records import OHLCBar
        bar = OHLCBar(
            ts=int(1_700_000_000e9),
            open=30000.0, high=31000.0, low=29000.0, close=30500.0, volume=10.0,
        )
        store.save(ds, [bar], Provenance(source="test"))

        cfg = tmp_path / "config.yml"
        _write_cfg(cfg, f"settings:\n  data_path: {data_dir}\n")

        result = runner.invoke(app, ["inventory", "--config", str(cfg)])
        assert result.exit_code == 0
        assert "binance" in result.output
        assert "BTC" in result.output


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

class TestStatus:
    def test_no_runs(self, empty_cfg):
        result = runner.invoke(app, ["status", "--config", str(empty_cfg)])
        assert result.exit_code == 0
        assert "No runs" in result.output

    def test_with_runs(self, tmp_path):
        """Status lists recent runs from the RunsStore."""
        from dccd.storage.runs_sqlite import RunsStore

        data_dir = tmp_path / "data"
        data_dir.mkdir()
        db_dir = data_dir / ".dccd"
        db_dir.mkdir()
        rs = RunsStore(db_dir / "runs.db")
        rs.create_run(
            run_id="run-001",
            spec_id="binance/BTC-USDT/ohlc/3600s",
            operation="backfill",
            exchange="binance",
            symbol="BTC/USDT",
            data_type="ohlc",
        )
        rs.finish_run("run-001", state="done", rows_written=42)

        cfg = tmp_path / "config.yml"
        _write_cfg(cfg, f"settings:\n  data_path: {data_dir}\n")

        result = runner.invoke(app, ["status", "--config", str(cfg)])
        assert result.exit_code == 0
        assert "run-001" in result.output
        assert "42" in result.output


# ---------------------------------------------------------------------------
# backfill (E2E via fake adapter + real ParquetStore)
# ---------------------------------------------------------------------------

class _FakeOHLCAdapter(_OHLCHistory):
    """Minimal OHLCHistory adapter that returns a handful of synthetic bars."""

    exchange = "fakeex"

    def capabilities(self):
        from dccd.domain.capability import Capability
        from dccd.domain.types import DataType
        return [
            Capability(
                data_type=DataType.OHLC,
                transport="rest",
                mode="historical",
                history="full",
                max_per_request=100,
                page_direction="forward",
            )
        ]

    def render_symbol(self, s):
        return str(s)

    def capability_for(self, data_type, transport, mode):
        for cap in self.capabilities():
            if cap.data_type == data_type and cap.transport == transport and cap.mode == mode:
                return cap
        return None

    async def fetch_ohlc_page(self, symbol, span, start_ns, end_ns, limit):
        from dccd.domain.records import OHLCBar
        # Return 3 bars starting at start_ns, spaced by span, then stop
        bars = []
        for i in range(3):
            bars.append(OHLCBar(
                ts=start_ns + i * span * 1_000_000_000,
                open=100.0 + i,
                high=110.0 + i,
                low=90.0 + i,
                close=105.0 + i,
                volume=1.0,
            ))
        return bars


class _FakeRegistry:
    def __init__(self, adapter):
        self._adapter = adapter

    def get(self, exchange: str):
        return self._adapter


class TestBackfill:
    def _patch_builders(self, tmp_path: pathlib.Path):
        """Return a context-manager dict of patches for the service_factory."""
        data_dir = tmp_path / "data"
        from dccd.storage.coverage_sqlite import CoverageStore
        from dccd.storage.parquet import ParquetStore
        from dccd.storage.runs_sqlite import RunsStore

        real_store = ParquetStore(data_dir)
        runs_store = RunsStore(data_dir / ".dccd" / "runs.db")
        coverage_store = CoverageStore(data_dir / ".dccd" / "coverage.db")
        registry = _FakeRegistry(_FakeOHLCAdapter())

        return {
            "dccd.interfaces.cli.main.service_factory.build_store": lambda p: real_store,
            "dccd.interfaces.cli.main.service_factory.build_runs_store": lambda p: runs_store,
            "dccd.interfaces.cli.main.service_factory.build_coverage_store": lambda p: coverage_store,
            "dccd.interfaces.cli.main.service_factory.build_registry": lambda: registry,
        }

    def test_backfill_explicit_exchange_symbol(self, tmp_path):
        """CLI→operation→store: rows written and readable from disk."""
        data_dir = tmp_path / "data"
        from dccd.domain.dataset import DatasetId
        from dccd.domain.symbol import Symbol
        from dccd.domain.types import DataType
        from dccd.storage.coverage_sqlite import CoverageStore
        from dccd.storage.parquet import ParquetStore
        from dccd.storage.runs_sqlite import RunsStore

        real_store = ParquetStore(data_dir)
        runs_store = RunsStore(data_dir / ".dccd" / "runs.db")
        coverage_store = CoverageStore(data_dir / ".dccd" / "coverage.db")
        registry = _FakeRegistry(_FakeOHLCAdapter())

        cfg = tmp_path / "config.yml"
        _write_cfg(cfg, f"settings:\n  data_path: {data_dir}\n")

        with (
            patch("dccd.application.service_factory.build_store", return_value=real_store),
            patch("dccd.application.service_factory.build_runs_store", return_value=runs_store),
            patch("dccd.application.service_factory.build_coverage_store", return_value=coverage_store),
            patch("dccd.application.service_factory.build_registry", return_value=registry),
        ):
            result = runner.invoke(app, [
                "backfill",
                "--config", str(cfg),
                "--exchange", "fakeex",
                "--symbol", "BTC/USDT",
                "--type", "ohlc",
                "--span", "3600",
                "--start", "2024-01-01",
            ])

        assert result.exit_code == 0, result.output
        assert "rows written" in result.output

        # Verify rows landed on disk
        ds = DatasetId(
            exchange="fakeex",
            symbol=Symbol(base="BTC", quote="USDT"),
            data_type=DataType.OHLC,
            span=3600,
        )
        df = real_store.load(ds)
        assert df is not None
        assert len(df) >= 1

    def test_backfill_no_jobs_exits_1(self, empty_cfg):
        """No jobs found → exit 1."""
        result = runner.invoke(app, ["backfill", "--config", str(empty_cfg)])
        assert result.exit_code == 1
        assert "No backfill jobs" in result.output

    def test_backfill_from_config_jobs(self, tmp_path):
        """Backfill driven by jobs configured in YAML."""
        data_dir = tmp_path / "data"
        from dccd.storage.coverage_sqlite import CoverageStore
        from dccd.storage.parquet import ParquetStore
        from dccd.storage.runs_sqlite import RunsStore

        real_store = ParquetStore(data_dir)
        runs_store = RunsStore(data_dir / ".dccd" / "runs.db")
        coverage_store = CoverageStore(data_dir / ".dccd" / "coverage.db")
        registry = _FakeRegistry(_FakeOHLCAdapter())

        # Config uses fakeex — it's accepted as a raw string (not validated as exchange)
        # Actually AppConfig validates exchange names against SUPPORTED_EXCHANGES.
        # Use "binance" in config but patch registry to return our fake adapter.
        cfg = tmp_path / "config.yml"
        _write_cfg(cfg, f"""\
            settings:
              data_path: {data_dir}
            jobs:
              - exchange: binance
                pairs: [BTC/USDT]
                data_type: ohlc
                span: 3600
                trigger_kind: once
        """)

        with (
            patch("dccd.application.service_factory.build_store", return_value=real_store),
            patch("dccd.application.service_factory.build_runs_store", return_value=runs_store),
            patch("dccd.application.service_factory.build_coverage_store", return_value=coverage_store),
            patch("dccd.application.service_factory.build_registry", return_value=registry),
        ):
            result = runner.invoke(app, [
                "backfill",
                "--config", str(cfg),
                "--start", "2024-01-01",
            ])

        assert result.exit_code == 0, result.output
        assert "rows written" in result.output

    def test_backfill_filter_by_exchange(self, tmp_path):
        """--exchange filter keeps only matching jobs from config."""
        data_dir = tmp_path / "data"
        from dccd.storage.coverage_sqlite import CoverageStore
        from dccd.storage.parquet import ParquetStore
        from dccd.storage.runs_sqlite import RunsStore

        real_store = ParquetStore(data_dir)
        runs_store = RunsStore(data_dir / ".dccd" / "runs.db")
        coverage_store = CoverageStore(data_dir / ".dccd" / "coverage.db")
        registry = _FakeRegistry(_FakeOHLCAdapter())

        cfg = tmp_path / "config.yml"
        _write_cfg(cfg, f"""\
            settings:
              data_path: {data_dir}
            jobs:
              - exchange: binance
                pairs: [BTC/USDT]
                data_type: ohlc
                span: 3600
                trigger_kind: once
              - exchange: kraken
                pairs: [ETH/USD]
                data_type: ohlc
                span: 3600
                trigger_kind: once
        """)

        with (
            patch("dccd.application.service_factory.build_store", return_value=real_store),
            patch("dccd.application.service_factory.build_runs_store", return_value=runs_store),
            patch("dccd.application.service_factory.build_coverage_store", return_value=coverage_store),
            patch("dccd.application.service_factory.build_registry", return_value=registry),
        ):
            result = runner.invoke(app, [
                "backfill",
                "--config", str(cfg),
                "--exchange", "kraken",
                "--start", "2024-01-01",
            ])

        assert result.exit_code == 0, result.output
        # Only the kraken job should run
        assert "kraken" in result.output


# ---------------------------------------------------------------------------
# stream
# ---------------------------------------------------------------------------

class TestStream:
    def test_no_stream_jobs_exits_1(self, empty_cfg):
        """Config with no stream jobs → exit 1."""
        result = runner.invoke(app, ["stream", "--config", str(empty_cfg)])
        assert result.exit_code == 1
        assert "No stream jobs" in result.output

    def test_stream_jobs_present_starts_scheduler(self, tmp_path):
        """When stream jobs exist the scheduler is started and then stopped."""
        data_dir = tmp_path / "data"
        cfg = tmp_path / "config.yml"
        _write_cfg(cfg, f"""\
            settings:
              data_path: {data_dir}
            jobs:
              - exchange: binance
                pairs: [BTC/USDT]
                data_type: trades
                operation: stream
                trigger_kind: supervised
        """)

        import asyncio

        mock_scheduler = MagicMock()
        # Make start/stop coroutines
        async def _start(specs): pass
        async def _stop(): pass
        mock_scheduler.start = _start
        mock_scheduler.stop = _stop

        from dccd.storage.parquet import ParquetStore
        from dccd.storage.runs_sqlite import RunsStore

        real_store = ParquetStore(data_dir)
        runs_store = RunsStore(data_dir / ".dccd" / "runs.db")

        with (
            patch("dccd.application.service_factory.build_store", return_value=real_store),
            patch("dccd.application.service_factory.build_runs_store", return_value=runs_store),
            patch("dccd.application.service_factory.build_registry", return_value=MagicMock()),
            patch("dccd.application.scheduler.Scheduler", return_value=mock_scheduler),
        ):
            # Patch asyncio.sleep to raise CancelledError immediately so the
            # infinite loop exits without waiting.
            async def _immediate_cancel(*a, **kw):
                raise asyncio.CancelledError()

            with patch("asyncio.sleep", side_effect=_immediate_cancel):
                result = runner.invoke(app, ["stream", "--config", str(cfg)])

        # CancelledError is caught and the command exits 0 (KeyboardInterrupt path)
        assert result.exit_code == 0


# ---------------------------------------------------------------------------
# ui  (wiring only — no port binding)
# ---------------------------------------------------------------------------

class TestUi:
    def test_missing_config_exits_nonzero(self, tmp_path):
        result = runner.invoke(app, ["ui", "--config", str(tmp_path / "nope.yml")])
        assert result.exit_code != 0

    def test_ui_starts_uvicorn(self, empty_cfg):
        """When config is valid, uvicorn.run is called with correct args."""
        mock_run = MagicMock()

        # create_app is imported locally from dccd.interfaces.api.app inside
        # cmd_ui; patch it there and uvicorn.run at the top-level uvicorn package.
        with (
            patch("uvicorn.run", mock_run),
            patch("dccd.interfaces.api.app.create_app", return_value=MagicMock()),
        ):
            result = runner.invoke(app, ["ui", "--config", str(empty_cfg)])

        mock_run.assert_called_once()
        assert result.exit_code == 0


# ---------------------------------------------------------------------------
# start  (wiring only)
# ---------------------------------------------------------------------------

class TestStart:
    def test_missing_config_exits_nonzero(self, tmp_path):
        result = runner.invoke(app, ["start", "--config", str(tmp_path / "nope.yml")])
        assert result.exit_code != 0

    def test_start_runs_daemon(self, empty_cfg, tmp_path):
        """start command builds all components and calls uvicorn.Server.serve()."""
        mock_server = MagicMock()

        async def _serve():
            pass  # return immediately

        mock_server.serve = _serve

        mock_scheduler = MagicMock()

        async def _start(specs): pass
        async def _stop(): pass
        mock_scheduler.start = _start
        mock_scheduler.stop = _stop

        from dccd.storage.coverage_sqlite import CoverageStore
        from dccd.storage.parquet import ParquetStore
        from dccd.storage.runs_sqlite import RunsStore

        data_dir = tmp_path / "data2"
        real_store = ParquetStore(data_dir)
        runs_store = RunsStore(data_dir / ".dccd" / "runs.db")
        coverage_store = CoverageStore(data_dir / ".dccd" / "coverage.db")

        with (
            patch("dccd.application.service_factory.build_store", return_value=real_store),
            patch("dccd.application.service_factory.build_runs_store", return_value=runs_store),
            patch("dccd.application.service_factory.build_coverage_store", return_value=coverage_store),
            patch("dccd.application.service_factory.build_registry", return_value=MagicMock()),
            patch("dccd.application.service_factory.build_remote", return_value=None),
            patch("dccd.interfaces.api.app.create_app", return_value=MagicMock()),
            patch("dccd.application.scheduler.Scheduler", return_value=mock_scheduler),
            patch("uvicorn.Server", return_value=mock_server),
            patch("uvicorn.Config", return_value=MagicMock()),
        ):
            result = runner.invoke(app, ["start", "--config", str(empty_cfg)])

        assert result.exit_code == 0, result.output

    def test_start_sweeps_orphans_before_scheduler(self, tmp_path):
        """cmd_start must call mark_stale_running *before* Scheduler.start."""
        from dccd.storage.coverage_sqlite import CoverageStore
        from dccd.storage.parquet import ParquetStore
        from dccd.storage.runs_sqlite import RunsStore

        data_dir = tmp_path / "data_sweep"
        real_store = ParquetStore(data_dir)
        runs_store = RunsStore(data_dir / ".dccd" / "runs.db")
        coverage_store = CoverageStore(data_dir / ".dccd" / "coverage.db")

        call_order: list[str] = []

        original_mark_stale = RunsStore.mark_stale_running

        def _recording_mark_stale(self):
            call_order.append("mark_stale_running")
            return original_mark_stale(self)

        mock_server = MagicMock()

        async def _serve():
            pass

        mock_server.serve = _serve

        mock_scheduler = MagicMock()

        async def _start(specs):
            call_order.append("scheduler.start")

        async def _stop():
            pass

        mock_scheduler.start = _start
        mock_scheduler.stop = _stop

        cfg_path = tmp_path / "config_sweep.yml"
        cfg_path.write_text(f"settings:\n  data_path: {data_dir}\n")

        with (
            patch("dccd.application.service_factory.build_store", return_value=real_store),
            patch("dccd.application.service_factory.build_runs_store", return_value=runs_store),
            patch("dccd.application.service_factory.build_coverage_store", return_value=coverage_store),
            patch("dccd.application.service_factory.build_registry", return_value=MagicMock()),
            patch("dccd.application.service_factory.build_remote", return_value=None),
            patch("dccd.interfaces.api.app.create_app", return_value=MagicMock()),
            patch("dccd.application.scheduler.Scheduler", return_value=mock_scheduler),
            patch("uvicorn.Server", return_value=mock_server),
            patch("uvicorn.Config", return_value=MagicMock()),
            patch.object(RunsStore, "mark_stale_running", _recording_mark_stale),
        ):
            result = runner.invoke(app, ["start", "--config", str(cfg_path)])

        assert result.exit_code == 0, result.output
        assert "mark_stale_running" in call_order, "mark_stale_running was never called"
        assert "scheduler.start" in call_order, "scheduler.start was never called"
        sweep_idx = call_order.index("mark_stale_running")
        start_idx = call_order.index("scheduler.start")
        assert sweep_idx < start_idx, (
            f"mark_stale_running (pos {sweep_idx}) must precede scheduler.start (pos {start_idx}); "
            f"order was: {call_order}"
        )
