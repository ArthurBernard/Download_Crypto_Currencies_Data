---
plan: epic-a-remote-server/02-verify-systemd
kind: leaf
status: planned
complexity: medium
depends: []
parallel: true
branch: chore/verify-systemd
pr: ""
---

# Verify the systemd unit

## Goal
Prove `deploy/dccd.service` installs and runs the daemon as a hardened,
auto-restarting service: correct paths, `User=dccd`, `Restart=on-failure`,
`XDG_CONFIG_HOME=/etc` → `/etc/dccd/config.yml`, writable data dir under the
sandbox of `ProtectSystem=strict` + `ReadWritePaths`.

## Files to change
- `deploy/dccd.service` — fix anything the verification surfaces: ensure
  `ReadWritePaths` covers the actual `data_path` (the install header uses
  `/var/lib/dccd/data`, the unit lists `/var/lib/dccd` — confirm consistent),
  `ExecStart` path note, and that `WorkingDirectory`/`StateDirectory=dccd` is
  considered (using `StateDirectory=` auto-creates `/var/lib/dccd` with correct
  perms — propose it if cleaner than manual `useradd --create-home`).
- The install header comment — align it with whatever the verification proves
  (exact `useradd`, config placement, `data_path`).

## Steps
1. `systemd-analyze verify deploy/dccd.service` — must report no errors.
2. Install on a test host/VM: copy to `/etc/systemd/system/`, create the `dccd`
   user (or rely on `DynamicUser=`/`StateDirectory=`), place a config at
   `/etc/dccd/config.yml` with `data_path: /var/lib/dccd/data`.
3. `systemctl daemon-reload && systemctl enable --now dccd`; check
   `systemctl status dccd` is active and the UI/`/health` answers.
4. Kill the process (`systemctl kill -s SIGKILL dccd` or `kill -9`) and confirm
   `Restart=on-failure` brings it back within `RestartSec`.
5. Confirm the data dir is writable under `ProtectSystem=strict` (a backfill writes
   Parquet under `/var/lib/dccd/data`).

## Tests
- No unit test (infra). `systemd-analyze verify` is the static gate; the live
  install is the real check.

## Verification on real data
- Real `systemctl` install + a kill→auto-restart cycle + a backfill that writes
  under the hardened data path.
- **If systemd/root is unavailable here**, run `systemd-analyze verify` (works
  without root), do the static review (perms, `ReadWritePaths` vs `data_path`,
  `Environment=XDG_CONFIG_HOME=/etc`), and flag that a live install must be done on
  a real host before Epic A closes.

## Closeout
- CHANGELOG (`Changed`/`Fixed`): "Verify + tighten `deploy/dccd.service`
  (ReadWritePaths/StateDirectory align with `data_path`; auto-restart confirmed)
  (#NN)".
- ADR: only if a real choice (e.g. `StateDirectory=`/`DynamicUser=` vs manual
  `useradd`) — record it then.
- Status/roadmap: deferred to last leaf (06).
