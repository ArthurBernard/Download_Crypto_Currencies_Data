"""Health monitor — subscribes to EventBus, writes metrics to RunsStore, sends alerts."""

from __future__ import annotations

import logging
import time
from collections import defaultdict

from dccd.application.events import Event, EventBus, StatusEvent
from dccd.storage.runs_sqlite import RunsStore

__all__ = ["HealthMonitor"]

logger = logging.getLogger(__name__)

# Minimum seconds between repeated alerts for the same job while it keeps failing.
_ALERT_COOLDOWN_S = 3600


class HealthMonitor:
    """Monitors job runs and fires webhook alerts on repeated failures.

    An alert fires when the consecutive-failure count first **crosses**
    ``max_consecutive_errors``.  While the job keeps failing, a follow-up alert
    fires at most once per ``_ALERT_COOLDOWN_S`` (1 hour) so a permanently-broken
    job does not flood a webhook.  The count (and cooldown) reset on the first
    success.

    Parameters
    ----------
    runs_store : RunsStore
    event_bus : EventBus
    webhook_url : str or None
    max_consecutive_errors : int
    """

    def __init__(
        self,
        runs_store: RunsStore | None,
        event_bus: EventBus,
        webhook_url: str | None = None,
        max_consecutive_errors: int = 3,
    ) -> None:
        self._store = runs_store
        self._bus = event_bus
        self._webhook = webhook_url
        self._max_errors = max_consecutive_errors
        self._consecutive: dict[str, int] = defaultdict(int)
        # Last monotonic timestamp at which an alert was sent per job key.
        self._last_alert_ts: dict[str, float] = {}
        # Last monotonic timestamp at which a webhook-send failure was logged per key.
        self._last_webhook_err_ts: dict[str, float] = {}
        event_bus.subscribe(self._on_event)

    def _on_event(self, event: Event) -> None:
        if not isinstance(event, StatusEvent):
            return
        # Count failures per *job*, not per run: a run_id is `{spec_id}@{run}` and
        # each backfill run is unique, so keying on run_id would never accumulate
        # across runs (only streams reuse `{spec_id}@stream`). Key on the spec_id
        # prefix so repeated failures of the same job trip the alert.
        key = event.run_id.split("@", 1)[0]
        if event.state == "failed":
            self._consecutive[key] += 1
            count = self._consecutive[key]
            if count == self._max_errors:
                # First crossing: always alert.
                self._alert(key, count)
            elif count > self._max_errors:
                # Still failing: re-alert only once per cooldown window.
                last = self._last_alert_ts.get(key, 0.0)
                if time.monotonic() - last >= _ALERT_COOLDOWN_S:
                    self._alert(key, count)
        elif event.state == "succeeded":
            self._consecutive[key] = 0
            # Reset cooldown so the next failure streak starts fresh.
            self._last_alert_ts.pop(key, None)
            self._last_webhook_err_ts.pop(key, None)

    def _alert(self, run_id: str, count: int) -> None:
        msg = f"dccd alert: {run_id} failed {count} times consecutively."
        logger.error(msg)
        self._last_alert_ts[run_id] = time.monotonic()
        if self._webhook:
            try:
                import json
                import urllib.request
                data = json.dumps({"text": msg}).encode()
                req = urllib.request.Request(
                    self._webhook,
                    data=data,
                    headers={"Content-Type": "application/json"},
                )
                with urllib.request.urlopen(req, timeout=5):
                    pass
            except Exception as exc:
                # Log webhook-send failures at most once per cooldown window.
                last_err = self._last_webhook_err_ts.get(run_id, 0.0)
                if time.monotonic() - last_err >= _ALERT_COOLDOWN_S:
                    logger.warning("Webhook alert failed: %s", exc)
                    self._last_webhook_err_ts[run_id] = time.monotonic()
