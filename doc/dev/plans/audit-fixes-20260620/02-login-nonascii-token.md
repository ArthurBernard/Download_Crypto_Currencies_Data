---
plan: audit-fixes-20260620/02-login-nonascii-token
kind: leaf
status: planned
complexity: low
depends: []
parallel: true
branch: fix/login-nonascii-token
pr: ""
---

# Make /login total on non-ASCII tokens

## Goal
`POST /login` must return the normal invalid-login response for a non-ASCII
submitted token, not a 500. `secrets.compare_digest` raises
`TypeError: comparing strings with non-ASCII characters is not supported` when
given a `str` with non-ASCII bytes; compare on `bytes` instead.

## Background
`dccd/interfaces/api/app.py:921`:
```python
if not token or not secrets.compare_digest(submitted, token):
```
`submitted` comes from the urlencoded form (`decode("utf-8", "replace")`) so it can
contain non-ASCII; `compare_digest(str, str)` then raises `TypeError` → unhandled →
500 (observed 2×/7d on the tailnet port, fail-closed, no auth bypass — but a 500
traceback instead of a clean rejection). `_valid_session` was checked and is **not**
affected (it does `sid in app.state.sessions`, no `compare_digest`). Line 921 is the
only `compare_digest` call.

## Files to change
- `dccd/interfaces/api/app.py` — line ~921: compare UTF-8 bytes:
  ```python
  if not token or not secrets.compare_digest(submitted.encode("utf-8"), token.encode("utf-8")):
  ```
  (`bytes` comparison is total; preserves constant-time semantics.)

## Steps
1. Encode both operands to `utf-8` bytes in the `compare_digest` call at the
   `POST /login` handler. No other logic changes.

## Tests
- `dccd/tests/v3/test_api.py` — add a TestClient case: with `ui_auth_token`
  configured, `POST /login` with `token=<non-ASCII string, e.g. "héllo€">` returns
  **200 and the login error page** (or whatever the existing wrong-token case
  asserts), **never 500**. Optionally assert the existing valid-token login still
  sets the session cookie (no regression).

## Verification on real data
- After release + deploy: `curl -s -o /dev/null -w "%{http_code}" -X POST
  --data 'token=hé€llo' http://127.0.0.1:8080/login` on the server returns 200 (not
  500), and the journal shows no new `TypeError: comparing strings with non-ASCII`
  ASGI exception.

## Closeout
- CHANGELOG (`Fixed`): "`POST /login` no longer returns 500 on a non-ASCII
  submitted token — `secrets.compare_digest` is given UTF-8 bytes, so a malformed
  token is rejected cleanly instead of raising `TypeError`. (#NN)"
- ADR: none — mechanical robustness fix (bytes compare is the obvious total form).
- Status/roadmap: no roadmap line; nothing to remove.
