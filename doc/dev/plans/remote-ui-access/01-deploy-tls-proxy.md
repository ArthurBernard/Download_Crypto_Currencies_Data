---
plan: remote-ui-access/01-deploy-tls-proxy
kind: leaf
status: done
complexity: medium
depends: []
parallel: false
branch: docs/tls-reverse-proxy
pr: "#NN"
---

# TLS + reverse proxy — document a secure remote front

## Goal

Document how to put the UI behind HTTPS so it can be reached from a laptop/phone
without ever exposing the plaintext API off-box. Caddy is the blessed/simple path;
nginx and Cloudflare Tunnel are alternatives. Docs-only — no code changes.

## Files to change
- `doc/source/how-to/expose-remote.rst` (new) — the guide. Sections:
  - *When you need this* — default bind is `127.0.0.1`; remote access is a conscious
    opt-in. Two safe shapes: (a) private overlay network (Tailscale) where the tailnet
    already gives transport encryption + identity, and (b) public exposure behind a
    TLS reverse proxy. Never bind `0.0.0.0` on a public IP without (a) or (b).
  - *Caddy (recommended)* — minimal `Caddyfile` reverse-proxying `your.host` →
    `127.0.0.1:8080`, automatic Let's Encrypt TLS. Keep `ui_host: 127.0.0.1` so only
    Caddy talks to dccd; set `ui_auth_token`. Note the SSE `?token=` only travels over
    TLS now. **Forwarding headers**: Caddy sets `X-Forwarded-Proto` and overwrites
    `X-Forwarded-For` by default — leaves 02/03 rely on these (Secure-cookie
    derivation + the rate-limit client key). When trusting XFF for rate-limiting, set
    `ui_trusted_proxy: true` (leaf 03) **only** because the proxy overwrites XFF.
  - *nginx (alternative)* — equivalent `server {}` block with `proxy_pass`,
    `proxy_set_header` for SSE (`proxy_buffering off;` + `Connection ''` so
    `text/event-stream` streams), TLS via certbot. **Must explicitly set**
    `proxy_set_header X-Forwarded-Proto $scheme;` and
    `proxy_set_header X-Forwarded-For $remote_addr;` (overwrite, not append, so a
    client can't inject a forged hop) — 02/03 depend on these.
  - *Cloudflare Tunnel (no public port)* — `cloudflared` tunnel to
    `http://127.0.0.1:8080`; no inbound port opened; TLS terminated at the edge.
  - *Tailscale (private, no public TLS needed)* — bind `ui_host: 0.0.0.0`, reach via
    the 100.x tailnet address; transport is already encrypted+authenticated. Combine
    with `ui_auth_token` as defence-in-depth. Cross-reference the testbox setup.
  - *Checklist* — token set; plaintext port not published publicly; SSE verified
    through the proxy (the `/api/events` stream must not be buffered).
- `doc/source/index.rst` — add `how-to/expose-remote` to the how-to toctree (after
  `how-to/deploy`).
- `doc/source/how-to/deploy.rst` — replace the short "Reaching the UI from another
  machine" section's body with a one-line pointer to the new `:doc:expose-remote`
  (keep the heading; avoid duplicating content).

## Steps
1. Write `expose-remote.rst`. Reuse the exact title-overline rule from `deploy.rst`
   (overline `=` line must be ≥ the title length, or Sphinx warns "Title overline too
   short").
2. Add the toctree entry and the deploy.rst pointer.
3. Use `.. code-block:: caddy` / `nginx` / `bash` fenced blocks; `sphinx-copybutton`
   is already enabled so commands get a copy button.
4. Cross-link `:doc:protect-ui` (token) and `:doc:sync-remote` (backups) where
   relevant, mirroring deploy.rst's "Operate it" cross-refs.

## Tests
- None (docs-only). The gate is the zero-warning docs build.

## Verification on real data
This is a deploy-doc leaf — verify the **documented procedure on the real testbox**,
not just that prose renders:
1. `cd doc && make clean && make html 2>&1 | grep -ciE 'warning:'` → must be `0`.
2. On `ssh dccd-testbox` (Tailscale): install Caddy, drop the documented `Caddyfile`
   pointing at the running `dccd start` (127.0.0.1:8080), reload Caddy.
3. From **this** machine over the tailnet: `curl -fsS https://<testbox>/health` →
   `{"status":"ok"}` over TLS; confirm the cert chain (`curl -v`, no `-k`).
4. Open `/api/events` through the proxy (`curl -N https://<testbox>/api/events?token=…`)
   and confirm SSE frames **stream** (not buffered until close) — this is the nginx/
   Caddy footgun the doc must prevent.
5. Confirm the plaintext `:8080` is **not** reachable off the testbox loopback
   (`curl --max-time 3 http://<testbox-tailnet-ip>:8080/health` should fail/refuse
   when `ui_host=127.0.0.1`). Record the commands+outputs in the PR.

## Closeout
- CHANGELOG (`Added`): "How-to guide for exposing the UI remotely behind TLS
  (Caddy/nginx/Cloudflare Tunnel/Tailscale), verified end-to-end on a real server
  (#NN)".
- ADR: append an entry — *Choice*: front remote exposure with a TLS reverse proxy
  (Caddy blessed) or a private overlay (Tailscale); keep `ui_host=127.0.0.1` behind a
  proxy. *Why*: never ship the API plaintext off-box; the existing token is
  defence-in-depth, not transport security. *Rejected*: binding `0.0.0.0` on a public
  IP with only the token; baking TLS into the app (proxies do it better).
- Status/roadmap: in `06-status.md` note the remote-exposure guide exists; **roadmap
  removal deferred to the last leaf (05)**.
