---
plan: remote-ui-access
kind: global
status: planning
roadmap: "## Epic B — View the UI remotely (PC + mobile)"
release_on_done: true
---

# Epic B — View the UI remotely (PC + mobile)

## Goal

Open the dashboard securely from a laptop or phone — not just `localhost` — without
ever exposing the API plaintext off-box. "Done" means: a documented TLS-fronted
deploy; a browser that can authenticate **without editing config** (so the token is
no longer leaked into the page on an ungated route); the API hardened for a hostile
network (rate-limited, no wildcard CORS, mutating routes provably token-gated, an
opt-in read-only mode); a UI usable on a narrow viewport; and a written threat model.
The bind/auth building blocks (`ui_host=0.0.0.0`, `ui_auth_token` Bearer,
`ui_allow_origins` CORS) already exist — this epic is wiring, hardening, and UX on
top of them.

## Security framing (drives the leaf order)

Today page routes (`/`, `/data`, …) are **not** gated and the server **injects the
auth token into the template** (`api/app.py` `_tpl_ctx` → `base.html` `DCCD_TOKEN`).
On a trusted `127.0.0.1` bind that's fine; the moment the UI is reachable remotely,
**anyone who loads a page receives the token**. So leaf **02 (browser auth/session)
is the linchpin** and must land before "harden" (03) and before the threat-model
note (05) can describe a true posture.

## Decomposition

1. **deploy-tls-proxy** — how-to: front the UI with HTTPS (Caddy primary; nginx and
   Cloudflare Tunnel as alternatives); the API never travels plaintext off-box.
   Docs-only, independent of the code leaves.
2. **ui-auth-session** — gate the **page** routes too: a token-prompt/login page that
   sets an HttpOnly cookie session; page routes redirect to it when unauthenticated;
   the `/api/*` guard accepts the cookie as well as Bearer/`?token=`. Stop injecting
   the raw token into the template. *The security linchpin.*
3. **harden-api-exposure** — rate-limit `/api/*`; a regression test asserting CORS is
   never wildcard; prove every mutating route is refused without a token; an opt-in
   `ui_readonly` mode that blocks mutating methods (read-only vs control).
4. **mobile-responsive** — narrow-viewport pass over Dashboard/Data/Historical/Live
   (tables → stacked/cards, tap targets, nav dropdowns); extend `ui_smoke.py` with a
   mobile-viewport run.
5. **threat-model-note** — write the assumptions (LAN vs internet, tunnel vs public,
   what the token does and doesn't protect) into the deploy how-to, reflecting the
   final posture from 01–03.

## Leaf checklist
- [ ] 01 deploy-tls-proxy — docs/tls-reverse-proxy — medium
- [ ] 02 ui-auth-session — feat/ui-auth-session — high
- [ ] 03 harden-api-exposure — feat/harden-api-exposure — high (depends on 02)
- [ ] 04 mobile-responsive — feat/ui-mobile-responsive — medium
- [ ] 05 threat-model-note — docs/threat-model — low (depends on 01, 02, 03)

## Dependencies
- 03 depends on 02 (roles/hardening build on the session auth).
- 05 depends on 01, 02, 03 (it documents the posture they establish).
- 01 and 04 are independent of the code-auth chain. 04 is logically parallelisable
  but is kept **serial** because it edits `base.html`, which 02 also touches — running
  them concurrently would collide. Execute 04 either before 02 or after it merges.

## Done criteria
- A real Ubuntu testbox serves the UI over **HTTPS** through the documented proxy;
  `curl` to the plaintext API port from off-box is refused/closed, the TLS front works.
- Loading any page **without** authenticating yields the login/token prompt, **not** a
  token-bearing page; after authenticating, the SPA drives the API via the cookie.
- `/api/*` is rate-limited (a burst past the limit returns 429); a test proves CORS is
  never wildcard and that mutating routes are 401 without a token; `ui_readonly: true`
  refuses mutating calls.
- The UI is usable at a 390-px viewport; `ui_smoke.py` passes a mobile-viewport run.
- The deploy how-to carries a threat-model section consistent with the above.
- Every leaf shipped as its own small PR; roadmap line removed on the last leaf.
