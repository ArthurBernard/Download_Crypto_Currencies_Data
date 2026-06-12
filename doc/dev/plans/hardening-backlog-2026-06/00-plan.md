---
plan: hardening-backlog-2026-06
kind: global
status: executing
roadmap: "## Hardening backlog (post-audit follow-ups, 2026-06-11)"
release_on_done: true
---

# Hardening backlog — purge×sync fix + small prod follow-ups

## Goal

Clear the bug-shaped items of the roadmap's **Hardening backlog**: the
purge × destructive-sync data-loss risk (now critical — the production server
carries the full 2020→present history and its hourly `rclone sync` would
delete purged files from the only backup), the duplicate manual-run guard,
runs.db retention, and ntfy-friendly alert formatting. The fifth backlog item
(**config export/load**) is a feature, not a bug — it stays on the roadmap for
a later epic.

## Decomposition

1. **remote-copy-not-sync** — make the off-box copy non-destructive
   (`rclone copy`), so purged local files survive on the remote for
   read-through restore. Unblocks `min_free_gb > 0`.
2. **duplicate-run-guard** — manual backfill triggers return the existing
   `run_id` instead of starting a concurrent duplicate for the same spec.
3. **runs-db-retention** — boot-time deletion of old terminal runs
   (configurable, default 90 days, `failed` kept) + `VACUUM`.
4. **ntfy-alert-format** — plain-text alert body (+ `X-Title`/`X-Priority`)
   for ntfy-style endpoints; JSON `{"text": …}` kept for Slack.

## Leaf checklist

- [x] 01 remote-copy-not-sync — fix/remote-copy-not-sync — medium
- [x] 02 duplicate-run-guard — fix/duplicate-run-guard — medium
- [x] 03 runs-db-retention — feat/runs-db-retention — medium
- [ ] 04 ntfy-alert-format — fix/ntfy-alert-format — medium

## Dependencies

- None — all four leaves touch disjoint files and may be executed in any
  order; 01 first because it is the standing data-loss risk.

## Done criteria

- All four leaf PRs merged into `develop`; `pytest` green; invariants intact.
- Leaf 01 proven by a purge→sync→restore round-trip test (and a live check
  that a file deleted locally is *not* deleted from a real rclone remote).
- The roadmap's Hardening backlog section reduced to the single
  **config export/load** feature item.
- `/release` suggested when the checklist is fully ticked (the purge fix
  should reach the production server promptly).
