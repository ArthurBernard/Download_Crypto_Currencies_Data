---
plan: epic-a-remote-server/01-verify-container
kind: leaf
status: planned
complexity: medium
depends: []
parallel: false
branch: chore/verify-container
pr: ""
---

# Verify the container image (build + run on the real box) and pin the base

## Goal
On `dccd-testbox`, prove the existing `Dockerfile` produces a working image: a
clean build, then a run with a mounted config + `/data` volume where `/health` and
the UI answer on `0.0.0.0`, and where a real backfill writes Parquet into the
volume. Pin the base image to a digest for reproducible builds.

## Context / ground truth
- `Dockerfile`: `FROM python:3.12-slim`, `XDG_CONFIG_HOME=/etc` (so config is read
  from `/etc/dccd/config.yml`), `pip install ".[daemon,ui]"`, `VOLUME ["/data"]`,
  `EXPOSE 8080`, `CMD ["start","--host","0.0.0.0","--port","8080"]`.
- `SettingsConfig` defaults: `data_path="./data/crypto"`, `ui_port=8080`,
  `ui_auth_token=None` (`dccd/application/config.py:41-45`). For the container the
  config must set `data_path: /data` and `ui_host: 0.0.0.0`.
- rclone is intentionally **not** installed in the image (only needed if
  `storage.remotes` is set) — so this leaf does **not** exercise sync.

## Target environment
`ssh dccd-testbox` (Ubuntu 24.04, passwordless sudo). Docker is **not yet
installed** — this leaf installs it (`docker.io`).

## Files to change
- `Dockerfile` — pin the base image: `FROM python:3.12-slim@sha256:<digest>` using
  the digest resolved on the box (`docker buildx imagetools inspect` or
  `docker inspect` after pull). Keep the human-readable tag in a trailing comment.
- (No `deploy.rst` here — leaf 06 owns it. Any build caveat → this leaf's Closeout
  for 06 to fold in.)

## Steps (run from the dev machine, driving the box)
1. **Install Docker on the box** (once):
   ```
   ssh dccd-testbox 'sudo apt-get update -qq && sudo DEBIAN_FRONTEND=noninteractive \
     apt-get install -y -qq docker.io && sudo systemctl enable --now docker && \
     sudo usermod -aG docker arthur && docker --version'
   ```
   (Group change needs a fresh session; until then prefix docker with `sudo`, or
   reconnect. `ssh dccd-testbox` reuses a ControlMaster — open a new master after
   the usermod so `docker` works without sudo, or just use `sudo docker`.)
2. **Ship the build context** to the box (no reliance on repo being public):
   ```
   rsync -az --delete \
     --exclude '.git' --exclude 'data' --exclude 'doc/_build' \
     --exclude '__pycache__' --exclude '*.pyc' --exclude '.pytest_cache' \
     ./ dccd-testbox:~/dccd-build/
   ```
3. **Build**: `ssh dccd-testbox 'cd ~/dccd-build && sudo docker build -t dccd:verify .'`
   — must exit 0.
4. **Write a throwaway config on the box** at `~/dccd-build/config.yml` with
   `settings.data_path: /data`, `ui_host: 0.0.0.0`, `ui_port: 8080`,
   `ui_auth_token: <test-token>`, and one tiny OHLC job (e.g. binance BTC/USDT 1h)
   so a backfill has something to write.
5. **Run** (detached), mounting config + a named volume:
   ```
   ssh dccd-testbox 'sudo docker run -d --name dccd-verify -p 8137:8080 \
     -v ~/dccd-build/config.yml:/etc/dccd/config.yml:ro \
     -v dccd-data:/data dccd:verify'
   ```
6. **Verify over Tailscale** from the dev machine:
   - `curl -fsS http://100.91.149.69:8137/health` → returns ok (200).
   - `curl -fsS -H "Authorization: Bearer <test-token>" http://100.91.149.69:8137/` →
     UI index HTML.
7. **Verify the volume is really written**: trigger the job
   (`curl -X POST .../api/jobs/run-all` with the bearer token, or
   `sudo docker exec dccd-verify dccd backfill …`), wait, then
   `ssh dccd-testbox 'sudo docker run --rm -v dccd-data:/data alpine \
     find /data -name "*.parquet"'` → at least one file.
8. **Pin the base image**: resolve the current `python:3.12-slim` digest on the box,
   set `FROM …@sha256:<digest>` in `Dockerfile`, rebuild → still exits 0 and runs.
9. **Tear down**: `sudo docker rm -f dccd-verify` (keep or remove `dccd-data` as
   needed). Record the digest in the commit message.

## Acceptance criteria
- `docker build` exits 0 both before and after the digest pin.
- `/health` returns 200 over the Tailscale IP; UI index loads with the bearer token.
- ≥1 `*.parquet` exists in the `dccd-data` volume after a backfill.
- `Dockerfile` `FROM` carries a `@sha256:` digest.

## Verification on real data
Steps 5–7 *are* the real verification (build → run → backfill → read what landed in
the volume). Back up nothing (throwaway box/volume). Capture the `curl /health`
output and the `find … *.parquet` listing into the PR.

## Risks / rollback
- Disk: the image + slim base is ~200–300 MB; the box has ~27 G free — fine.
- Rollback: `sudo docker rm -f dccd-verify; sudo docker rmi dccd:verify`; the
  `~/dccd-build` dir and `dccd-data` volume can be deleted.

## Closeout
- CHANGELOG (`Changed`): "Pin the Docker base image to a digest; verified
  `docker build`+`run` on a live host (mounted config + `/data` volume, `/health`
  + UI reachable, backfill writes to the volume) (#NN)".
- ADR: only if the digest-pin-vs-tag choice is worth recording — else "none".
- Status/roadmap: deferred to leaf 06.
