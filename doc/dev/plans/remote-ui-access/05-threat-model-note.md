---
plan: remote-ui-access/05-threat-model-note
kind: leaf
status: planned
complexity: low
depends: [01, 02, 03]
parallel: false
branch: docs/threat-model
pr: ""
---

# Threat-model note in the deploy how-to

## Goal

Write down the security assumptions for remote exposure so a reader knows exactly what
is and isn't protected. Docs-only; depends on 01–03 because it must describe the
**final** posture (TLS front, cookie session + gated pages, rate-limit/read-only).

## Files to change
- `doc/source/how-to/expose-remote.rst` (created in leaf 01) — add a **Threat model**
  section near the end. Cover:
  - *Trust boundaries*: localhost-only (default, no auth needed) vs private overlay
    (Tailscale: transport already encrypted+authenticated) vs public internet (must be
    behind a TLS reverse proxy; never plaintext).
  - *What the token/session protects*: API authZ for a **single shared secret** — not
    per-user identity, not multi-tenant. The cookie session is opaque and HttpOnly; the
    token is never templated into pages (post leaf 02).
  - *What it does NOT protect*: it is not a substitute for TLS (use a proxy or the
    overlay); a shared token has no per-user revocation beyond rotating it; in-process
    rate-limit/session state resets on restart (acceptable for a single-node daemon).
  - *Residual risks & mitigations*: `?token=` in SSE URLs can appear in proxy logs —
    prefer the cookie path for browsers (default after 02); set `ui_readonly` for
    view-only shares; bound `ui_rate_limit`; keep `data_path` under the service's
    `StateDirectory`.
  - *Recommended postures table*: LAN-only / Tailnet / Public — for each: bind, TLS,
    token, read-only suggestion.
- `doc/source/how-to/deploy.rst` — ensure its "Reaching the UI remotely" pointer (set
  in leaf 01) also references the threat-model section anchor.

## Steps
1. Write the Threat model section, consistent with what 01–03 actually shipped (read
   the merged leaves, don't restate the plan).
2. Add the postures table (`.. list-table::` or a simple grid).
3. Re-check cross-references resolve.

## Tests
- None (docs-only). Gate: zero-warning docs build.

## Verification on real data
- `cd doc && make clean && make html 2>&1 | grep -ciE 'warning:'` → `0`.
- Read the rendered page top-to-bottom and confirm every claim matches the **shipped**
  behaviour of leaves 01–03 (e.g. "token not in page source" is true; `ui_readonly`
  exists; rate-limit header is `Retry-After`). No aspirational statements.

## Closeout
- CHANGELOG (`Added`): "Threat-model section in the remote-exposure how-to (trust
  boundaries, what the token/session protects, recommended postures) (#NN)".
- ADR: none — documents decisions already recorded by leaves 01–03 (state "none —
  documents prior ADRs" at finish).
- Status/roadmap: **this is the last leaf** — set `00-plan.md` `status: done`, **remove
  the Epic B block from `doc/dev/07-roadmap.md`** and update the "Suggested sequence"
  (mark B done), update `06-status.md` to reflect Epic B complete, archive the whole
  `doc/dev/plans/remote-ui-access/` tree to `_archive/`, and **suggest `/release`**.
