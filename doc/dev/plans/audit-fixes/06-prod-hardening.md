---
plan: audit-fixes/06-prod-hardening
kind: leaf
status: executing
complexity: medium
depends: []
parallel: false
branch: chore/prod-hardening
pr: ""
---

# Prod — off-box backup, alert webhook, systemd limits on arthurserver

## Goal

Epic C shipped rclone sync but prod uses none of it: **the collected data is
backed up nowhere**, the alert webhook is null (HealthMonitor alerts go into
the void), and the systemd unit has no `MemoryMax`/`TimeoutStopSec` (62 s
observed stop vs 90 s default before SIGKILL). Configure all three on
arthurserver and align the repo's deploy templates/docs.

**Needs user input before execution**: the rclone remote destination
(provider + credentials) and the webhook target (ntfy/Discord/Slack/…).
If unavailable when the leaf runs, do the repo-side template/doc work, apply
the systemd limits, and report the two blocked items back instead of
inventing endpoints.

## Files to change

- `deploy/dccd.service` — add `TimeoutStopSec=120` and a commented
  `MemoryMax=` example (prod RSS observed ~830 MB; suggest 1.5G), short
  comment on why.
- `doc/source/how-to/deploy*` and `how-to/sync-remote*` — a "production
  checklist" subsection: remote backup ON, webhook ON, memory/stop limits.
- arthurserver (`ssh dccd-testbox`, ops — no repo diff): rclone remote in
  the dccd config (`remotes:` + `sync_interval`), `alerts.webhook_url`,
  systemd drop-in with the limits, `systemctl daemon-reload && restart`.

## Steps

1. Repo: service template + how-to edits (PR-able regardless of ops).
2. Server: configure rclone remote (user-provided), run one manual sync
   cycle, verify the coverage manifest lands on the remote.
3. Server: set webhook, force a failure (e.g. temporary bogus job) or use
   any test hook HealthMonitor offers, confirm delivery.
4. Server: systemd drop-in (`TimeoutStopSec=120`, `MemoryMax=1.5G`),
   restart, confirm clean stop < 120 s and steady RSS.
5. `pytest` (repo side untouched logically, must stay green).

## Tests

- None beyond the suite (config/ops leaf). The how-to build must keep
  Sphinx at 0 warnings (`cd doc && make html`).

## Verification on real data

- `rclone lsf <remote>` shows the synced tree growing after the scheduled
  cycle; restore path: delete one local dataset dir on an **isolated copy**
  (never prod data without backup-first) and confirm read-through restore.
- Webhook: one delivered test alert visible in the receiving channel.
- `systemctl show dccd | grep -E 'TimeoutStopSec|MemoryMax'` shows the
  limits; journal shows a clean stop on restart.

## Closeout

- CHANGELOG (`Changed`): "Deploy template ships explicit
  `TimeoutStopSec`/`MemoryMax` guidance; production checklist added to the
  deploy/sync how-tos (#NN)"
- ADR: none — ops + doc.
- Status/roadmap: tick leaf; update `06-status.md` prod paragraph (backed
  up off-box, alerts live).
- Memory note: update `reference_test_server` memory (backup + webhook now
  configured).
