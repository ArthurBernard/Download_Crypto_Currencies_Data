---
plan: remote-ui-access/02-ui-auth-session
kind: leaf
status: planned
complexity: high
depends: []
parallel: false
branch: feat/ui-auth-session
pr: ""
---

# Browser auth — login page + cookie session (stop leaking the token into pages)

## Goal

Let a browser (incl. a phone) authenticate **without editing config**, and stop
serving the raw `ui_auth_token` inside every page. When `ui_auth_token` is set:
unauthenticated page loads get a **login page**; submitting the token sets an
**HttpOnly session cookie**; page routes and the `/api/*` guard both accept that
cookie. When no token is set (default localhost), behaviour is unchanged.

## Security problem being fixed
`api/app.py` `_tpl_ctx()` injects `ui_auth_token` into the template
(`base.html` `const DCCD_TOKEN = "{{ auth_token }}"`), and page routes are ungated.
So any client that can reach a page **receives the token** — unacceptable once the UI
is remote. After this leaf the browser never sees the token value; it holds an opaque
session cookie instead.

## Files to change
- `dccd/interfaces/api/app.py`
  - Add a small session layer. On successful token submit, mint a random opaque
    session id (`secrets.token_urlsafe(32)`), store it in an in-process dict
    `app.state.sessions` (value = creation-ns) with an **absolute TTL** (default 7
    days; prune expired ids on each check so the dict can't grow unbounded), and set
    it as cookie `dccd_session` with `HttpOnly`, `SameSite=Lax`, `Secure` **derived
    from the request scheme** (`request.url.scheme == "https"` or
    `X-Forwarded-Proto: https` so it works behind the leaf-01 proxy), `Path=/`,
    and `Max-Age` matching the TTL.
  - **CSRF** — because the API guard will accept the cookie, mutating calls become
    CSRF-reachable. `SameSite=Lax` is the mitigation: it withholds the cookie on
    **cross-site POST/DELETE** (all mutating routes are POST/DELETE), while still
    allowing top-level GET navigation. Do **not** use `SameSite=None`. Document this
    in a code comment and assert it in tests (below). (`Strict` would also work but
    breaks following an external link into an authed page; `Lax` is the right balance.)
  - Routes:
    - `GET /login` → render `login.html` (a tiny token-prompt form posting to
      `/login`). If already authed, redirect to `/`.
    - `POST /login` → compare submitted token to `ui_auth_token` with
      `secrets.compare_digest` (timing-safe); on match set cookie + redirect to `next`
      or `/`; on mismatch re-render with an error (HTTP 401).
      **Open-redirect guard**: only accept `next` if it is a **local path** —
      `next.startswith("/")` **and not** `next.startswith("//")` (and no scheme/`\`);
      otherwise fall back to `/`. A helper `_safe_next(next)` + a unit test for
      `//evil.com`, `https://evil`, `/data` cases.
    - `POST /logout` → drop the session, clear the cookie, redirect to `/login`.
  - **Page-route gate**: a helper `_page_authed(request)` (no token configured → always
    True; else cookie present in `app.state.sessions`). Wrap the page routes so an
    unauthed load returns `RedirectResponse(f"/login?next={path}", 303)` instead of the
    page. Keep `/login`, `/static/*`, `/health` ungated.
  - **API guard**: extend `_auth_guard` to also accept a valid `dccd_session` cookie
    (in addition to `Authorization: Bearer` and `?token=`), so the SPA's `fetch`/SSE
    calls authorise via the cookie — no token in JS needed.
  - **Stop injecting the token**: in `_tpl_ctx`, drop `auth_token` from the context
    (or pass `""`). The front-end no longer needs it (cookie is sent automatically).
- `dccd/interfaces/ui/templates/login.html` (new) — minimal page extending a bare
  shell (or standalone): a centered form, password-type input named `token`, submit;
  shows an error banner when `error` is set. No nav.
- `dccd/interfaces/ui/templates/base.html`
  - Remove `const DCCD_TOKEN = "{{ auth_token }}"` and the
    `opts.headers['Authorization'] = 'Bearer ' + DCCD_TOKEN` line. `fetch` calls go to
    same-origin `/api/*`; the browser sends the cookie automatically (ensure
    `credentials:'same-origin'` — it's the default for same-origin, but set it
    explicitly on the wrapper for clarity).
  - **SSE**: `EventSource('/api/events')` cannot set headers, but **does** send
    same-origin cookies automatically, so drop the `?token=` query for the browser
    case. (The `?token=` path stays supported server-side for `curl`/non-browser.)
  - Add a small "Logout" affordance in the nav, shown only when a session is active
    (pass a `bool authed` into the context to conditionally render it).
- `dccd/interfaces/api/app.py` `_tpl_ctx` — add `"authed": _page_authed(request)` for
  the logout button.

## Steps
1. Implement the session store + cookie helpers (mind `Secure` derivation behind a
   proxy via `X-Forwarded-Proto`).
2. Add `/login` (GET/POST) and `/logout`; write `login.html`.
3. Gate page routes via `_page_authed`; keep `/login`,`/static`,`/health` open.
4. Extend `_auth_guard` to accept the session cookie.
5. Strip `DCCD_TOKEN`/Bearer-from-JS and the SSE `?token=` from `base.html`; rely on
   the cookie; add the conditional Logout.
6. Keep the no-token (localhost default) path a pure pass-through — no login, no gate.

## Tests
- `dccd/tests/v3/test_api.py` (TestClient):
  - With `ui_auth_token` set: `GET /` unauthenticated → 303 redirect to `/login`;
    `GET /login` → 200 with the form.
  - `POST /login` wrong token → 401; correct token → 303 + `Set-Cookie: dccd_session=…`
    with `HttpOnly`. Reusing that cookie: `GET /` → 200, and an `/api/*` call with the
    cookie (no Bearer) → 200; without it → 401.
  - `POST /logout` clears the cookie; subsequent `GET /` → 303 to `/login`.
  - **No token configured**: `GET /` → 200 (no redirect); `/api/*` → 200 (unchanged).
  - Assert the rendered pages **no longer contain the token string** (regression for
    the leak): fetch `/` authed and assert the configured token value is absent from
    the HTML body.
  - Bearer header and `?token=` continue to authorise `/api/*` (back-compat).
  - **CSRF**: a cross-site-style POST (cookie present but `Sec-Fetch-Site`/`Origin`
    cross-site — or simply: the cookie is `SameSite=Lax` so a real cross-site POST
    wouldn't carry it) must not succeed on cookie alone. Assert `Set-Cookie` contains
    `SameSite=Lax` and `HttpOnly`; assert no route sets `SameSite=None`.
  - **Open redirect**: `_safe_next("//evil.com")` and `_safe_next("https://evil")`
    fall back to `/`; `_safe_next("/data")` is preserved.

## Verification on real data
On the testbox behind the leaf-01 TLS proxy (or Tailscale), with a token set:
1. From a **phone browser**, load `https://<testbox>/` → land on the login page;
   submit the token → reach the dashboard; navigate Data/Historical/Live; confirm SSE
   liveness updates flow (cookie-authed EventSource) and **view source shows no token**.
2. `curl -i https://<testbox>/` (no cookie) → 303 `/login`; `curl` the login POST,
   capture the cookie jar, re-request `/` and `/api/jobs` with the jar → 200.
3. Confirm `Secure` is set on the cookie when reached over HTTPS (and not set on plain
   `http://127.0.0.1` so localhost dev still works). Record commands+outputs in the PR.

## Closeout
- CHANGELOG (`Added`): "Browser login page + HttpOnly cookie session so the UI
  authenticates without editing config; the auth token is no longer injected into
  served pages (#NN)". (`Security`/`Fixed`): "page routes are now gated when
  `ui_auth_token` is set".
- ADR: *Choice*: opaque in-process cookie session + `/login`; gate page routes; accept
  the cookie in the API guard; stop templating the token. *Why*: remote exposure made
  the templated token a leak; cookies are the browser-native, header-less-SSE-friendly
  auth. *Rejected*: signed-JWT/stateless (overkill for a single shared token); keeping
  the templated token (leaks); HTTP Basic (worse UX, no logout).
- Status/roadmap: in `06-status.md` flip the auth-UX gap; roadmap removal deferred to
  leaf 05.
