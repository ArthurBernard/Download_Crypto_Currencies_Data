---
plan: epic-a-remote-server/05-secrets-config
kind: leaf
status: planned
complexity: low
depends: [01]
parallel: false
branch: docs/secrets-injection
pr: ""
---

# Secrets/config: keep config.yml out of the image, document injection

## Goal
Confirm no secret is baked into the image (config.yml is *mounted*, not COPYed —
the `Dockerfile` already does this) and document how to inject `ui_auth_token` (and
`rclone.conf` for sync) via env/volume at run time.

## Files to change
- `doc/source/how-to/deploy.rst` — **owned by leaf 06**; write the secrets section
  as a self-contained block here and hand it to 06, OR (if 06 not yet merged) add a
  short `Secrets` subsection to the existing `how-to/protect-ui.rst` and let 06
  cross-link. Decide at execution based on ordering; do not create `deploy.rst`
  here.
- `examples/config.example.yml` — confirm it carries **no real token** (placeholder
  only) and a comment pointing at env injection.

## Steps
1. Static check: `grep -i COPY Dockerfile` shows config is **not** copied; the
   `VOLUME`/mount + `XDG_CONFIG_HOME=/etc` is the only config path. Confirm.
2. Document injection patterns:
   - Docker: `-v $HOME/.config/rclone:/root/.config/rclone:ro`,
     `-v ./config.yml:/etc/dccd/config.yml:ro`, and `ui_auth_token` via the
     mounted config (kept out of the image/VCS) — note env-substitution isn't
     parsed by the config loader unless it is (verify `config.py`; if not, say so
     and recommend the mounted-file pattern).
   - systemd: config at `/etc/dccd/config.yml` with `0600 root:dccd` perms;
     `rclone.conf` under the service user's `$XDG_CONFIG_HOME`.
3. Confirm `examples/config.example.yml` has only placeholders.

## Tests
- None (docs + static verification). If `config.py` is found to support env
  substitution, add/confirm a small unit test for it; otherwise note the limitation.

## Verification on real data
- Build the image (or reuse leaf 01's) and `grep` its layers / `docker history` to
  confirm no `config.yml`/token is present in the image. Run with an injected
  token and confirm `/api/*` enforces Bearer (ties to `protect-ui.rst`).

## Closeout
- CHANGELOG (`Changed`/docs): "Document secret injection for deploy (token +
  rclone.conf via mounted volume, never baked into the image) (#NN)".
- ADR: "none" unless `config.py` env-substitution is added (then record it).
- Status/roadmap: deferred to last leaf (06).
