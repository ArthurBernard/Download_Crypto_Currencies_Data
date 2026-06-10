---
plan: remote-ui-access/03-harden-api-exposure
kind: leaf
status: done
complexity: high
depends: [02]
parallel: false
branch: feat/harden-api-exposure
pr: "#NN"
---

# Harden /api for a hostile network — rate limit, CORS proof, read-only role

## Goal

Make the API safe(r) to expose: bound request rate on `/api/*`, a regression test
that CORS is never wildcard, proof that every mutating route is refused without auth,
and an opt-in **read-only** mode that blocks mutating methods (read-only vs control).

## Files to change
- `dccd/application/config.py` `SettingsConfig` — add `ui_readonly: bool = False`,
  `ui_rate_limit: int = 0` (requests/sec per client IP on `/api/*`; `0` = disabled,
  the localhost default), and `ui_trusted_proxy: bool = False` (whether to trust
  `X-Forwarded-For` for the client key — see the spoofing note below). Validate
  `ui_rate_limit >= 0`. Document all three in the model docstring.
- `dccd/interfaces/api/app.py`
  - **Rate limiter**: a tiny in-process token-bucket per client key, applied in a
    middleware ordered **before** `_auth_guard`, only when `ui_rate_limit > 0` and the
    path starts with `/api/`. Over-limit → `429` JSON `{"detail":"rate limited"}` with
    a `Retry-After` header. Reuse the existing `transport/ratelimit.py` token-bucket if
    it's import-safe here; otherwise a minimal local bucket (keep it dependency-free).
  - **Client key / XFF spoofing guard** (important): `X-Forwarded-For` is
    attacker-controlled when the app is reachable directly, so trusting it blindly lets
    a single client forge unlimited keys and bypass the limit. Therefore: use
    `request.client.host` **by default**; honour `X-Forwarded-For` (first hop) **only
    when `ui_trusted_proxy=True`** — i.e. the operator asserts the app is reachable
    *only* through the leaf-01 reverse proxy. Document that `ui_trusted_proxy` must be
    set **only** behind a proxy that overwrites XFF.
  - **Read-only mode**: when `settings.ui_readonly` is True, refuse mutating requests
    to `/api/*` — block `POST/PUT/PATCH/DELETE` (the mutating verbs; all job CRUD,
    backfill run/cancel, stream start/stop are POST/DELETE) with `403`
    `{"detail":"read-only"}`. `GET`/`HEAD`/`OPTIONS` pass. Apply in the guard.
  - **CORS**: no code change expected (already opt-in, non-wildcard at app.py:226) —
    but add an assertion path the test can exercise.
- `doc/source/how-to/expose-remote.rst` (created in leaf 01) — add a short
  "Hardening" subsection documenting `ui_rate_limit` and `ui_readonly`.

## Steps
1. Add the two settings + validation (non-negative int; bool).
2. Implement the rate-limit middleware (before auth) gated on `ui_rate_limit>0`.
3. Add the read-only check in `_auth_guard` (after auth succeeds, before dispatch).
4. Document both in the expose-remote how-to.

## Tests
- `dccd/tests/v3/test_api.py`:
  - **CORS regression**: build an app with `ui_allow_origins=[]` → no
    `access-control-allow-origin: *` ever returned; with a specific origin set → that
    origin echoed, never `*`. (Assert wildcard is impossible.)
  - **Rate limit**: app with `ui_rate_limit=2`; a burst of N>bucket GET `/api/jobs`
    from the same client returns at least one `429` with `Retry-After`; a different
    client key is unaffected; with `ui_rate_limit=0` no 429 ever.
  - **XFF trust**: with `ui_trusted_proxy=False` (default), a forged `X-Forwarded-For`
    header does **not** create a fresh bucket (keying ignores XFF → same
    `request.client.host` still rate-limited). With `ui_trusted_proxy=True`, distinct
    XFF values get distinct buckets.
  - **Read-only**: app with `ui_readonly=True` + valid auth → `POST /api/jobs/create`
    → 403 `read-only`; `GET /api/jobs` → 200. With `ui_readonly=False` the POST works
    (existing behaviour). Confirm read-only is enforced **after** auth (a 401 still
    wins for unauthenticated mutating calls).
  - **Mutating-routes-need-auth sweep**: parametrise over the mutating endpoints
    (`/api/jobs/{create,delete,update}`, `/api/jobs/{run,run-all}`,
    `/api/backfill/{id}` DELETE, stream start/stop) and assert each is `401` without a
    token when `ui_auth_token` is set.

## Verification on real data
On the testbox behind the proxy, token set:
1. Set `ui_rate_limit: 5`; hammer `/api/jobs` (`for i in $(seq 50); do curl -s -o
   /dev/null -w '%{http_code}\n' …; done`) → observe `429`s appear; confirm normal
   browsing still works under the limit.
2. Set `ui_readonly: true`, reload; from the browser the Run/Start/Delete buttons get
   `403`; `GET` views still render. Flip back to control; mutations work again.
3. Confirm the proxy's `X-Forwarded-For` is the key (rate limit is per real client,
   not per-proxy) — two devices get independent buckets. Record outputs in the PR.

## Closeout
- CHANGELOG (`Added`): "`ui_rate_limit` (token-bucket on `/api/*`) and `ui_readonly`
  (block mutating methods) settings for hardened remote exposure (#NN)".
  (`Security`): "regression test proving CORS is never wildcard and every mutating
  route requires the token".
- ADR: *Choice*: in-process per-IP token-bucket + read-only verb gate, both opt-in
  (off on localhost). *Why*: exposure needs abuse-resistance and a safe view-only
  share without standing up Redis/an external WAF. *Rejected*: external rate-limiter
  dependency; per-route role decorators (heavier than a verb gate for a single shared
  token).
- Status/roadmap: note hardening in `06-status.md`; roadmap removal deferred to leaf 05.
