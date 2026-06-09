---
plan: epic-a-remote-server/01-verify-container
kind: leaf
status: planned
complexity: medium
depends: []
parallel: true
branch: chore/verify-container
pr: ""
---

# Verify the container image (build + run) and pin the base

## Goal
Prove the existing `Dockerfile` produces a working image: a clean build, then a
run with a mounted config + `/data` volume where the web UI is reachable on
`0.0.0.0`. Pin/refresh the base image so builds are reproducible.

## Files to change
- `Dockerfile` — pin the base image to a digest (`FROM python:3.12-slim@sha256:…`)
  or, if a digest is undesirable, document why and keep the tag; add a
  `HEALTHCHECK` only if leaf 04 hasn't claimed it (coordinate — leave healthcheck
  to 04, just confirm `/health` responds here).
- `doc/source/how-to/deploy.rst` — **do not create here** (leaf 06 owns it); if a
  build caveat surfaces, jot it in the leaf Closeout for 06 to fold in.

## Steps
1. `docker build -t dccd:verify .` from the repo root — must succeed.
2. Create a throwaway config (`data_path: /data`, `ui_host: 0.0.0.0`, a test
   `ui_auth_token`), run:
   `docker run --rm -p 8137:8080 -v "$PWD/config.yml:/etc/dccd/config.yml:ro" -v dccd-data:/data dccd:verify`.
3. Confirm `GET http://127.0.0.1:8137/health` returns ok and the UI index loads
   (with the Bearer token).
4. Pin the base image digest in `Dockerfile`; rebuild to confirm it still works.

## Tests
- No unit test (infra). The verification *is* the real build+run below.
- If the digest pin is added, ensure `docker build` still passes — record the
  digest used in the commit message.

## Verification on real data
- Real build + run (above). Hit `/health` and the UI on the published port; confirm
  the mounted `/data` volume is written (trigger a tiny backfill via the UI or
  `docker exec … dccd backfill …` and see a Parquet file appear under the volume).
- **If Docker is unavailable in this environment**, say so explicitly (like the
  rclone stand-in precedent), do the static review of the `Dockerfile` (paths,
  `XDG_CONFIG_HOME=/etc` → `/etc/dccd/config.yml`, `EXPOSE`/`CMD` port match), and
  flag that a real build must be run on a Docker host before Epic A closes.

## Closeout
- CHANGELOG (`Changed`): "Pin the Docker base image to a digest for reproducible
  builds; verified `docker build`+`run` with mounted config and `/data` volume
  (#NN)" — adjust to what was actually done.
- ADR: only if a non-trivial choice (e.g. digest-pin vs tag) — else "none".
- Status/roadmap: deferred to last leaf (06).
