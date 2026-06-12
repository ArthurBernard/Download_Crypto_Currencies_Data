---
plan: hardening-backlog-2026-06/04-ntfy-alert-format
kind: leaf
status: planned
complexity: medium
depends: []
parallel: false
branch: fix/ntfy-alert-format
pr: ""
---

# ntfy-friendly alert formatting

## Goal

`HealthMonitor._alert` POSTs `{"text": msg}` as JSON; on ntfy the phone
notification shows the raw JSON blob. Send a plain-text body (ntfy renders
it as the message) with `X-Title: dccd` and `X-Priority: high` headers,
keeping the JSON `{"text": …}` shape for Slack-compatible endpoints
(detected by host: `hooks.slack.com`).

## Files to change

- `dccd/application/monitor.py` — extract the webhook send from `_alert`
  into a small private helper, e.g. `_post_webhook(self, msg: str) -> None`:
  parse `urllib.parse.urlsplit(self._webhook).hostname`; if it is
  `hooks.slack.com` (or ends with it) → current JSON behaviour; otherwise →
  `data = msg.encode()`, headers `{"Content-Type": "text/plain",
  "X-Title": "dccd", "X-Priority": "high"}`. Keep the existing try/except +
  send-failure cooldown exactly as is. Update the class docstring's webhook
  paragraph.
- `dccd/tests/v3/` — monitor tests: check whether a monitor test file
  exists (`grep -rl HealthMonitor dccd/tests/v3/`); extend it, else create
  `test_monitor_webhook.py`.

## Steps

1. Refactor the send into `_post_webhook` (behaviour-preserving first), then
   add the host-based format switch.
2. Tests below; `pytest` + `ruff check dccd/`.

## Tests

Monkeypatch `urllib.request.urlopen` to capture the `Request` object:

- `test_webhook_plain_text_for_ntfy` — webhook `https://ntfy.sh/topic`:
  body == the raw message bytes (no JSON), `Content-Type: text/plain`,
  `X-Title` and `X-Priority` headers present.
- `test_webhook_json_for_slack` — webhook
  `https://hooks.slack.com/services/X/Y/Z`: body is JSON with `text` key,
  `Content-Type: application/json` (today's behaviour preserved).
- `test_webhook_send_failure_still_cooled_down` — keep/extend the existing
  failure-cooldown coverage if it exists; otherwise assert a raising
  urlopen doesn't propagate.

## Verification on real data

- One-shot script against the **real production ntfy topic** (the user's
  phone — one test message is acceptable and proves the chain):
  instantiate `HealthMonitor` with the prod `webhook_url` read from the
  server config over ssh, call the private send helper with a clearly
  labelled test message ("dccd format test — ignore"), and confirm HTTP 200.
  Visually: the phone shows the plain message, not a JSON blob (note this
  for the user to confirm; the 200 + correct headers is the automated
  check).

## Closeout

- CHANGELOG `Fixed`: "webhook alerts send a plain-text body with
  `X-Title`/`X-Priority` for ntfy-style endpoints (the phone showed a raw
  JSON blob); Slack webhooks (`hooks.slack.com`) keep the JSON `{\"text\"}`
  payload (#NN)"
- ADR: none — formatting fix; host-based detection noted in the docstring.
- Status/roadmap: remove the ntfy bullet; this empties the Hardening
  backlog down to the config export/load feature item (leave that one).
