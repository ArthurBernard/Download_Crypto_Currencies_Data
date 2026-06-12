---
plan: hardening-backlog-2026-06/01-remote-copy-not-sync
kind: leaf
status: executing
complexity: medium
depends: []
parallel: false
branch: fix/remote-copy-not-sync
pr: ""
---

# Off-box copy must be non-destructive (purge × sync data loss)

## Goal

`RemoteStorage.sync_one` runs `rclone sync` — a *mirror* that deletes remote
files absent locally — while `purge_to_free_space` deletes old already-synced
local files expecting the remote to keep them for read-through restore
(`RemoteStorage.restore`). Purge a file and the next hourly sync deletes the
only remaining copy. Switch the upload to `rclone copy` (never deletes on the
remote): local = hot tier, remote = complete archive. This is the tiered
semantic Epic C intended, and it unblocks ever setting `min_free_gb > 0`.

## Files to change

- `dccd/storage/remote.py` — `sync_one`: `["rclone", "sync", …]` →
  `["rclone", "copy", …]`. Update the module/class/method docstrings: the
  remote is an *archive superset* of the local store, files are never deleted
  remotely (deliberate — restore depends on it; remote cleanup is a manual
  operation).
- `dccd/tests/v3/test_remote_sync.py` — adjust assertions that expect the
  `sync` verb; add the round-trip regression test (below).
- `doc/source/how-to/sync-remote.rst` — wherever it describes mirror
  semantics, state archive-superset semantics instead (grep for "sync"/
  "mirror"/"deleted" and align; keep edits surgical).

## Steps

1. Change the rclone verb in `sync_one` and rewrite the touched docstrings.
2. Fix existing tests that assert the command line (they monkeypatch
   `subprocess.run` — check `test_remote_sync.py` for the pattern).
3. Add `test_purge_then_sync_preserves_remote_copy` (below).
4. `pytest` + `ruff check dccd/` until green.

## Tests

- `dccd/tests/v3/test_remote_sync.py::test_sync_one_uses_copy_verb` — the
  subprocess command starts `rclone copy` (not `sync`).
- `test_purge_then_sync_preserves_remote_copy` — the round-trip guard, all
  local with a directory standing in for the remote: (a) store dir with two
  parquet files, "remote" dir seeded with both (simulate a completed sync);
  (b) `purge_to_free_space` with a `free_fn` forcing the deletion of the
  oldest file; (c) run `sync_one` with `subprocess.run` replaced by a fake
  that *applies* the rclone verb semantics to the dirs (copy = add/overwrite
  only; sync = add/overwrite + delete extraneous) — with `copy` the purged
  file must still exist on the "remote" afterwards, and `restore()` (same
  fake, copy semantics) must bring it back locally. The fake's
  sync-vs-copy behaviour is what would have caught the original bug.

## Verification on real data

- On a throwaway local rclone remote (e.g. `rclone config create tmploc
  local` pointing at a /tmp dir — no network needed): create a small store,
  `sync_all` it, delete one local parquet (simulating purge), `sync_all`
  again, and verify the file **still exists** under the remote path; then
  `restore()` it and verify content equality with the original. Run via the
  repo venv against the real rclone binary.

## Closeout

- CHANGELOG `Fixed`: "off-box sync no longer mirrors deletions: `rclone
  copy` replaces `rclone sync`, so locally purged files survive on the
  remote for read-through restore — enabling `min_free_gb` no longer risks
  deleting the only copy (#NN)"
- ADR: yes — "Remote is an archive superset, not a mirror": choice
  (`rclone copy`), why (purge+restore contract; full history now lives on
  the production store), rejected alternatives (`sync --backup-dir`:
  restores need a second path lookup; purge-aware exclude lists: stateful
  and fragile). Note the accepted trade-off: deleting data from the remote
  becomes a manual operation.
- Status/roadmap: remove the purge×sync roadmap bullet (added 2026-06-12);
  drop the "purge stays off until fixed" caveat from `06-status.md`.
