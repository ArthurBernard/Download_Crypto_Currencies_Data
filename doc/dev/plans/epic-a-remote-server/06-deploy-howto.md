---
plan: epic-a-remote-server/06-deploy-howto
kind: leaf
status: planned
complexity: medium
depends: [01, 02, 03, 04, 05]
parallel: false
branch: docs/deploy-howto
pr: ""
---

# Deploy how-to: one blessed end-to-end path (validated on the box)

## Goal
Write `doc/source/how-to/deploy.rst` documenting **one blessed deployment path**
end-to-end, **followed literally on `dccd-testbox`** to prove it works, folding in
everything verified by leaves 01–05. Record the systemd-vs-Docker decision as an
ADR. This leaf **closes Epic A**.

## Context / ground truth
- The how-to dir holds `add-exchange · analyse · deep-trades · protect-ui ·
  schedule-daily · sync-remote` (`doc/source/how-to/`). Find the toctree that lists
  them (`doc/source/how-to/index.rst` or the parent `index.rst`) and add `deploy`.
- By the time this leaf runs, leaves 01–05 have established the *verified facts*:
  venv `/opt/dccd/venv` + `StateDirectory=dccd` (02), digest-pinned image (01),
  reboot-safe (03), `HealthMonitor` wired + `/health` healthcheck (04), secret
  injection (05). The how-to is the synthesis, not new claims.

## Decision to record (ADR)
Pick **one** primary path and mark it *Recommended*, the other as the alternative:
- **systemd** (recommended for a long-lived home/VPS box): venv at `/opt/dccd`,
  `dccd` user, `StateDirectory`, journald, auto-restart, reboot-safe.
- **Docker** (recommended for a containerised/ephemeral host): digest-pinned image,
  named `/data` volume, `HEALTHCHECK`, restart policy.
Record the rationale in `doc/dev/03-decisions.md`.

## Files to change
- `doc/source/how-to/deploy.rst` (new) — a single coherent walkthrough:
  1. Prereqs (a server, Tailscale/SSH or public reachability).
  2. **Recommended path** end-to-end (provision → install → config + **secret
     injection** (05) → start/enable → verify `/health` → confirm auto-restart).
  3. The **alternative path** (the other target), condensed.
  4. Ops: `/health` + healthcheck/watchdog, resource limits, journald logs, webhook
     alerts (04).
  5. Restart/reboot safety note (03). Cross-link `protect-ui`, `sync-remote`.
  6. **Old-CPU caveat** (ADR 2026-06-09): on servers without AVX2 the default
     `polars` wheel crashes (SIGILL) — Docker users add
     `--build-arg POLARS_VARIANT=polars-lts-cpu`; venv users
     `pip install polars-lts-cpu` (after uninstalling `polars`). Quick check:
     `grep -o -m1 avx2 /proc/cpuinfo` — empty ⇒ use the LTS-CPU variant.
- the how-to toctree — add `deploy`.
- `doc/dev/06-status.md` — flip Epic A from pending → done (deploy section).
- `doc/dev/07-roadmap.md` — **remove the entire Epic A block** (via `/finish-task`).

## Steps
1. Confirm leaves 01–05 are merged (their PRs closed) — the source material.
2. Draft `deploy.rst`; then **execute it verbatim** on `dccd-testbox` from a clean
   state (tear down prior leaf artefacts first: remove the service/venv/containers),
   following only what the doc says. Any step that doesn't work as written → fix the
   doc until a clean run stands up a working, self-restarting dccd.
3. `cd doc && make html` → **0 warnings**; all cross-links resolve.
4. `/finish-task` (last leaf): remove the Epic A roadmap block, set the global
   `00-plan.md` `status: done`, archive the tree to `_archive/plans/`, suggest
   `/release`.
5. Post-epic hygiene: note in the PR that `sudoers.d/99-arthur-nopasswd` on the box
   can be revoked, and the box reset, now that Epic A is validated.

## Acceptance criteria
- `deploy.rst` exists, is in the toctree, and `make html` is 0 warnings.
- The doc was run **verbatim on the box** from a clean state and produced a working,
  auto-restarting dccd (capture the final `systemctl is-active` / `docker inspect`
  + `/health` 200 into the PR).
- ADR recorded; `06-status.md` shows Epic A done; `07-roadmap.md` Epic A block gone.

## Verification on real data
The "execute the doc verbatim on a clean box" run *is* the verification — it proves
the how-to is sufficient and correct, not just plausible.

## Risks / rollback
- A doc that "looks right" but skips a step is the classic failure — the verbatim
  clean-box run is the guard. Don't shortcut it.

## Closeout
- CHANGELOG (`Added`): "`how-to/deploy` — blessed, host-validated path to run dccd
  unattended (systemd/Docker, `/health` healthcheck, restart/reboot safety, secret
  injection, webhook alerts), completing Epic A (#NN)".
- ADR: "Blessed deployment path = <systemd|Docker> primary, the other as
  alternative — why."
- Status: Epic A done in `06-status.md`. Roadmap: remove the Epic A section. Then
  **suggest `/release`** (global `release_on_done: true`).
