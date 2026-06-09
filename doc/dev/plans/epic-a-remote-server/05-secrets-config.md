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

# Secrets/config: prove nothing is baked into the image, document injection

## Goal
Prove (not assume) that no secret lands inside the image, and document how to
inject `ui_auth_token` and `rclone.conf` at run time for both deploy targets.

## Context / ground truth
- `Dockerfile` `COPY`s only `pyproject.toml README.md` + `dccd/` — **never**
  `config.yml`; config is read from `/etc/dccd/config.yml` via `XDG_CONFIG_HOME=/etc`
  at run time (mounted), and `VOLUME ["/data"]` holds data. So the design is already
  correct — this leaf **verifies it on the real image** and writes it down.
- `SettingsConfig.ui_auth_token` defaults `None` (`config.py:45`); `protect-ui.rst`
  already documents Bearer auth. Whether the YAML loader does **env substitution**
  is unverified → check `config.py` `_load_cfg`/loader; if it does *not*, the blessed
  pattern is a mounted file kept out of VCS (not `${ENV}` in the YAML).

## Files to change
- Do **not** create `deploy.rst` here (leaf 06 owns it). Write the **Secrets**
  section as a self-contained block in this leaf's body / a scratch file and hand it
  to leaf 06; if leaf 06 isn't merged yet and a home is needed sooner, add a short
  `Secrets` subsection to `doc/source/how-to/protect-ui.rst` and let 06 cross-link.
- `examples/config.example.yml` — confirm only a **placeholder** token + a comment
  pointing at injection; fix if a real-looking value is present.

## Steps (driving the box, reuse leaf 01's image)
1. **Static**: `grep -n COPY Dockerfile` → confirms config is not copied.
2. **Prove on the built image**:
   ```
   ssh dccd-testbox 'sudo docker history --no-trunc dccd:verify | grep -i -E "token|config.yml|secret" || echo "no secret in history"'
   ssh dccd-testbox 'sudo docker run --rm --entrypoint sh dccd:verify -c "ls -la /etc/dccd 2>/dev/null; test -f /etc/dccd/config.yml && echo BAKED || echo CLEAN"'
   ```
   Expect `no secret in history` and `CLEAN` (config only appears when mounted).
3. **Check env substitution** in `dccd/application/config.py` (the loader). If
   supported, document `${UI_AUTH_TOKEN}`; if not, document the mounted-file pattern
   and say so explicitly (no hand-waving).
4. **Document injection** for both targets:
   - **Docker**: `-v ./config.yml:/etc/dccd/config.yml:ro` (token lives in that file,
     kept out of git), `-v $HOME/.config/rclone:/root/.config/rclone:ro` for sync.
   - **systemd**: `/etc/dccd/config.yml` `0640 root:dccd`; `rclone.conf` under the
     service user's config dir; never commit either.
5. Confirm `examples/config.example.yml` is placeholder-only.

## Acceptance criteria
- `docker history` shows no token/config; the image has **no** `/etc/dccd/config.yml`
  unless mounted (`CLEAN`).
- The injection patterns are documented for Docker **and** systemd, matching the
  loader's real capabilities (env-subst or mounted-file).
- `examples/config.example.yml` carries no real secret.

## Verification on real data
Step 2 inspects the **actual built image** on the box (history + filesystem), not a
claim. Capture both command outputs into the PR.

## Risks / rollback
- If env substitution is found and documented, add/confirm a tiny unit test for it;
  otherwise note the limitation so users don't put `${ENV}` in YAML expecting it to
  expand.

## Closeout
- CHANGELOG (`Changed`/docs): "Document secret injection for deploy (token +
  `rclone.conf` via mounted volume, never baked into the image); verified on the
  built image (#NN)".
- ADR: "none" unless env-substitution is added to the loader (then record it).
- Status/roadmap: deferred to leaf 06.
