---
plan: epic-a-remote-server/02-verify-systemd
kind: leaf
status: planned
complexity: medium
depends: []
parallel: false
branch: chore/verify-systemd
pr: ""
---

# Verify the systemd unit (real system-wide install on the box)

## Goal
Install `deploy/dccd.service` system-wide on `dccd-testbox` running as a dedicated
`dccd` user, prove it starts, auto-restarts (`Restart=on-failure`), and that the
hardening (`ProtectSystem=strict` + `ReadWritePaths`) still lets it write its data.
Fix the `ExecStart` path assumption surfaced by the static gate.

## Context / ground truth (already verified)
- `systemd-analyze verify deploy/dccd.service` **fails** with
  `Command /usr/local/bin/dccd is not executable: No such file or directory`.
  The unit hard-codes `ExecStart=/usr/local/bin/dccd start`, which only exists for a
  system-wide `pip install`. On Ubuntu 24.04 (PEP 668, externally-managed Python) a
  system pip install needs `--break-system-packages` — **a venv is the blessed
  path**. The unit header already hints "adjust ExecStart to your venv".
- Unit today: `User=dccd`, `Group=dccd`, `Environment=XDG_CONFIG_HOME=/etc`
  (→ `/etc/dccd/config.yml`), `Restart=on-failure`, `RestartSec=5`,
  `ProtectSystem=strict`, `ProtectHome=true`, `PrivateTmp=true`,
  `ReadWritePaths=/var/lib/dccd`. Install header uses `data_path: /var/lib/dccd/data`.

## Target environment
`ssh dccd-testbox`. systemd 255, passwordless sudo, PID 1 = systemd.

## Files to change
- `deploy/dccd.service`:
  - Set `ExecStart=/opt/dccd/venv/bin/dccd start` (the blessed venv path), and/or
    add a comment block making the path the one explicit thing to adjust.
  - Add `StateDirectory=dccd` (systemd auto-creates `/var/lib/dccd` with correct
    ownership for `User=dccd` — cleaner than a manual `useradd --create-home`),
    keeping `ReadWritePaths=/var/lib/dccd` consistent with `data_path`.
  - Consider `WorkingDirectory=/var/lib/dccd`.
- The install-header comment — align with the venv + `StateDirectory` approach
  actually verified.

## Steps (driving the box)
1. **Provision the venv + install dccd** on the box:
   ```
   ssh dccd-testbox 'sudo mkdir -p /opt/dccd && sudo python3 -m venv /opt/dccd/venv && \
     sudo /opt/dccd/venv/bin/pip install -q --upgrade pip'
   rsync -az --delete --exclude .git --exclude data --exclude doc/_build \
     --exclude __pycache__ ./ dccd-testbox:~/dccd-src/
   ssh dccd-testbox 'sudo /opt/dccd/venv/bin/pip install -q "~/dccd-src/[daemon,ui]" || \
     (cd ~/dccd-src && sudo /opt/dccd/venv/bin/pip install -q ".[daemon,ui]") && \
     /opt/dccd/venv/bin/dccd --help >/dev/null && echo DCCD_INSTALLED'
   ```
2. **Create the service user** (or rely on `StateDirectory`/`DynamicUser`):
   `ssh dccd-testbox 'sudo useradd --system --no-create-home --shell /usr/sbin/nologin dccd || true'`
3. **Static gate**: copy the edited unit and
   `ssh dccd-testbox 'systemd-analyze verify /path/to/dccd.service'` → **no errors**
   (this is the gate the original failed; it must pass with the venv ExecStart).
4. **Place config** at `/etc/dccd/config.yml` (`data_path: /var/lib/dccd/data`,
   `ui_host: 0.0.0.0`, a test token, one tiny OHLC job), perms `0640 root:dccd`.
5. **Install + start**:
   ```
   ssh dccd-testbox 'sudo cp ~/dccd-src/deploy/dccd.service /etc/systemd/system/ && \
     sudo systemctl daemon-reload && sudo systemctl enable --now dccd && \
     sleep 3 && systemctl is-active dccd'
   ```
6. **Verify running**: `systemctl status dccd` active; `journalctl -u dccd -n 30`
   shows "Daemon running"; `curl -fsS http://100.91.149.69:8080/health` → 200.
7. **Auto-restart**: `sudo systemctl kill -s SIGKILL dccd` then after `RestartSec`
   confirm `systemctl is-active dccd` is `active` again and the PID changed.
8. **Hardening writes**: trigger the configured backfill; confirm a `*.parquet`
   lands under `/var/lib/dccd/data` (i.e. `ProtectSystem=strict` +
   `ReadWritePaths`/`StateDirectory` permit the write) — read it back:
   `ssh dccd-testbox 'sudo find /var/lib/dccd/data -name "*.parquet" | head'`.

## Acceptance criteria
- `systemd-analyze verify` of the edited unit → exit 0, no errors.
- `systemctl is-active dccd` = active; `/health` 200 on `:8080`.
- After `SIGKILL`, the service is auto-restarted within ~`RestartSec`.
- A backfill writes ≥1 Parquet under `/var/lib/dccd/data` despite the hardening.

## Verification on real data
Steps 6–8 are the real verification (live service + kill→restart + a backfill that
writes under the hardened path, read back). Capture `systemctl status`,
`journalctl`, and the `find … *.parquet` output into the PR.

## Risks / rollback
- The unit's `ProtectSystem=strict` can block writes if `ReadWritePaths` /
  `data_path` disagree — that's exactly what step 8 catches; fix by aligning them.
- Rollback: `sudo systemctl disable --now dccd; sudo rm /etc/systemd/system/dccd.service \
  /etc/dccd/config.yml; sudo userdel dccd; sudo rm -rf /opt/dccd /var/lib/dccd`.

## Closeout
- CHANGELOG (`Changed`/`Fixed`): "`deploy/dccd.service`: ExecStart→venv path (passes
  `systemd-analyze verify`), `StateDirectory=dccd` aligned with `data_path`;
  verified live install + auto-restart + writes under hardening on a real host (#NN)".
- ADR: record "venv at `/opt/dccd` + `StateDirectory=dccd` over system-wide pip /
  manual useradd — why" (PEP 668, clean ownership).
- Status/roadmap: deferred to leaf 06.
