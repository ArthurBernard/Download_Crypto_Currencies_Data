"""Tests for HealthMonitor._post_webhook format switching (ntfy vs Slack)."""

from __future__ import annotations

import json
import logging
import urllib.request

import pytest

from dccd.application.events import EventBus
from dccd.application.monitor import HealthMonitor

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _FakeResp:
    """Context-manager stub returned by the monkeypatched urlopen."""

    def __enter__(self) -> "_FakeResp":
        return self

    def __exit__(self, *args: object) -> bool:
        return False


def _make_monitor(webhook_url: str) -> HealthMonitor:
    bus = EventBus()
    return HealthMonitor(None, bus, webhook_url=webhook_url, max_consecutive_errors=3)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestWebhookFormat:
    """_post_webhook sends the right format depending on the endpoint hostname."""

    def test_webhook_plain_text_for_ntfy(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """ntfy endpoint receives a plain-text body with X-Title and X-Priority."""
        captured: list[urllib.request.Request] = []

        def fake_urlopen(req: urllib.request.Request, timeout: int = 0) -> _FakeResp:
            captured.append(req)
            return _FakeResp()

        monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

        monitor = _make_monitor("https://ntfy.sh/my-topic")
        msg = "dccd: test alert for ntfy"
        monitor._post_webhook(msg, run_id="test-job")

        assert len(captured) == 1
        req = captured[0]

        # Body must be the raw message bytes, not JSON.
        assert req.data == msg.encode()

        # urllib title-cases header names: "Content-Type" → "Content-type".
        assert req.get_header("Content-type") == "text/plain"
        assert req.get_header("X-title") == "dccd"
        assert req.get_header("X-priority") == "high"

    def test_webhook_json_for_slack(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Slack endpoint receives JSON {"text": msg} with Content-Type application/json."""
        captured: list[urllib.request.Request] = []

        def fake_urlopen(req: urllib.request.Request, timeout: int = 0) -> _FakeResp:
            captured.append(req)
            return _FakeResp()

        monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

        monitor = _make_monitor("https://hooks.slack.com/services/X/Y/Z")
        msg = "dccd: test alert for slack"
        monitor._post_webhook(msg, run_id="test-job")

        assert len(captured) == 1
        req = captured[0]

        # Body must be JSON with a "text" key.
        payload = json.loads(req.data)
        assert payload == {"text": msg}

        assert req.get_header("Content-type") == "application/json"

        # Must NOT carry ntfy-specific headers.
        assert req.get_header("X-title") is None
        assert req.get_header("X-priority") is None

    def test_webhook_send_failure_does_not_propagate(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failing urlopen must not raise out of _post_webhook."""

        def raising_urlopen(req: urllib.request.Request, timeout: int = 0) -> _FakeResp:
            raise OSError("connection refused")

        monkeypatch.setattr(urllib.request, "urlopen", raising_urlopen)

        monitor = _make_monitor("https://ntfy.sh/my-topic")
        # Must not raise.
        monitor._post_webhook("some alert", run_id="test-job")

    def test_webhook_send_failure_logs_warning(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failing urlopen logs a warning (first failure, no prior cooldown)."""
        from dccd.application.monitor import _ALERT_COOLDOWN_S

        def raising_urlopen(req: urllib.request.Request, timeout: int = 0) -> _FakeResp:
            raise OSError("boom")

        monkeypatch.setattr(urllib.request, "urlopen", raising_urlopen)
        # Freeze monotonic at a value that satisfies (now - last_err >= cooldown)
        # when last_err is the default 0.0.
        monkeypatch.setattr(
            "dccd.application.monitor.time.monotonic", lambda: float(_ALERT_COOLDOWN_S + 1)
        )

        warnings: list[str] = []
        orig_warning = logging.Logger.warning

        def capture_warning(self: logging.Logger, msg: object, *args: object, **kwargs: object) -> None:
            warnings.append(str(msg) % args if args else str(msg))
            return orig_warning(self, msg, *args, **kwargs)

        monkeypatch.setattr(logging.Logger, "warning", capture_warning)

        monitor = _make_monitor("https://ntfy.sh/topic")
        monitor._post_webhook("msg", run_id="job1")

        assert any("boom" in w for w in warnings)

    def test_slack_subdomain_uses_json(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A subdomain of hooks.slack.com (e.g. something.hooks.slack.com) uses JSON."""
        captured: list[urllib.request.Request] = []

        monkeypatch.setattr(
            urllib.request,
            "urlopen",
            lambda req, timeout=0: (captured.append(req), _FakeResp())[1],
        )

        monitor = _make_monitor("https://sub.hooks.slack.com/webhook")
        monitor._post_webhook("msg", run_id="job")

        payload = json.loads(captured[0].data)
        assert "text" in payload
