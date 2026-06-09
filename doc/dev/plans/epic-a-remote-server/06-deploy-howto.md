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

# Deploy how-to: one blessed end-to-end path

## Goal
Write `doc/source/how-to/deploy.rst` documenting **one blessed deployment path**
end-to-end, folding in everything verified/hardened by leaves 01–05. Record the
systemd-vs-Docker decision as an ADR. This is the leaf that **closes Epic A**:
remove the roadmap items and suggest `/release`.

## Files to change
- `doc/source/how-to/deploy.rst` (new) — the blessed path (recommend **systemd**
  as primary for a long-running home/VPS server, **Docker** as the containerised
  alternative — or flip, per the decision): provisioning, config placement +
  secrets injection (from 05), start/enable, `/health` + restart safety (03),
  ops/limits/alerting (04). Cross-link `protect-ui.rst`, `sync-remote.rst`.
- the how-to toctree (the index that lists `add-exchange`, `sync-remote`, …) — add
  `deploy`.
- `doc/dev/06-status.md` — flip Epic A from pending to done (tooling/deploy).
- `doc/dev/07-roadmap.md` — **remove all Epic A items** (done by `/finish-task`).

## Steps
1. Confirm leaves 01–05 are merged (their verified facts are the source material).
2. Write `deploy.rst` as a single coherent walkthrough; keep both targets but mark
   one **Recommended** with a one-paragraph rationale (the ADR).
3. Add `deploy` to the how-to toctree; `cd doc && make html` must be 0 warnings.
4. At `/finish-task`: this is the **last leaf** → remove the Epic A roadmap block,
   mark the global `00-plan.md` done, archive the tree, suggest `/release`.

## Tests
- None (docs). Gate = `cd doc && make html` → 0 warnings; all cross-links resolve.

## Verification on real data
- Follow the written how-to **literally** on a clean target (VM/host) to stand up a
  self-restarting dccd; if the sandbox can't, state which steps were executed vs
  reasoned, consistent with the honesty bar set by leaves 01/02.

## Closeout
- CHANGELOG (`Added`): "`how-to/deploy` guide — blessed end-to-end path to run dccd
  unattended on a server (systemd/Docker, healthcheck, restart safety, secrets,
  alerting), completing Epic A (#NN)".
- ADR: "Blessed deployment path = <systemd|Docker> primary, the other documented as
  alternative — why."
- Status: Epic A done in `06-status.md`. Roadmap: remove the Epic A section.
