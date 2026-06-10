==================================================
Expose the UI remotely (TLS + reverse proxy)
==================================================

The UI binds ``127.0.0.1`` by default — reachable only from the box itself. Reaching
it from a laptop or phone is a **conscious opt-in**: the API must never travel
plaintext off-box. Two safe shapes:

- **Private overlay network** (Tailscale/WireGuard) — the tailnet already gives you
  transport encryption *and* device identity, so you can bind to it directly.
- **Public exposure behind a TLS reverse proxy** (Caddy, nginx, Cloudflare Tunnel) —
  the proxy terminates HTTPS and forwards to dccd on loopback.

Never bind ``ui_host: 0.0.0.0`` on a public IP without one of the above. The
``ui_auth_token`` (:doc:`protect-ui`) is **defence-in-depth, not transport security** —
it authorises API calls but does not encrypt anything.

.. note::

   Always set ``ui_auth_token`` as well. Behind a proxy or on a tailnet it is the
   second factor that stops anyone who reaches the port from driving the API.

Caddy (recommended)
===================

Caddy gets you automatic Let's Encrypt TLS in two lines. Keep
``ui_host: 127.0.0.1`` so **only Caddy** talks to dccd:

.. code-block:: text

   your.host.example {
       reverse_proxy 127.0.0.1:8080
   }

That is the whole ``Caddyfile``. Caddy provisions the certificate, terminates TLS,
and forwards to dccd. It sets ``X-Forwarded-Proto: https`` and overwrites
``X-Forwarded-For`` with the real client address by default — dccd relies on both
(the first to mark the session cookie ``Secure``, the second as the rate-limit client
key when proxy trust is enabled). Server-Sent Events stream through Caddy with no
extra config.

Reload after editing: ``sudo systemctl reload caddy`` (or ``caddy reload``).

nginx (alternative)
===================

SSE needs buffering off and the forwarding headers set explicitly (nginx does not
add them for you):

.. code-block:: nginx

   server {
       listen 443 ssl;
       server_name your.host.example;

       # ssl_certificate / ssl_certificate_key via certbot

       location / {
           proxy_pass http://127.0.0.1:8080;
           proxy_http_version 1.1;

           # SSE: do not buffer the event stream
           proxy_buffering off;
           proxy_set_header Connection '';

           # dccd relies on these; overwrite (not append) so a client
           # cannot inject a forged X-Forwarded-For hop.
           proxy_set_header X-Forwarded-Proto $scheme;
           proxy_set_header X-Forwarded-For $remote_addr;
           proxy_set_header Host $host;
       }
   }

Provision the certificate with certbot (``certbot --nginx``).

Cloudflare Tunnel (no public port)
==================================

``cloudflared`` dials out to Cloudflare, so you open **no inbound port** and TLS is
terminated at the edge:

.. code-block:: bash

   cloudflared tunnel login
   cloudflared tunnel create dccd
   # route a hostname to the local service:
   cloudflared tunnel route dns dccd your.host.example
   cloudflared tunnel --url http://127.0.0.1:8080 run dccd

Keep ``ui_host: 127.0.0.1``. This is a good fit for a box behind NAT with no public
IP.

Tailscale (private, no public TLS needed)
=========================================

On a tailnet the transport is already encrypted and authenticated, so you can bind
the UI to the tailnet and reach it by the device's ``100.x`` address:

.. code-block:: yaml

   settings:
     ui_host: 0.0.0.0          # reachable on the tailnet interface
     ui_auth_token: "a-long-random-string"

Reach it at ``http://<device>.<tailnet>.ts.net:8080`` or the ``100.x`` IP. Because
there is no public TLS proxy here, the cookie is not marked ``Secure`` — that is
acceptable because the tailnet itself is the encrypted boundary. Still set
``ui_auth_token`` as defence-in-depth. (This machine's throwaway test box is reached
exactly this way.)

Verify the front
================

After wiring a proxy, confirm TLS *and* that SSE actually streams (the classic
buffering footgun):

.. code-block:: bash

   curl -fsS https://your.host.example/health           # {"status":"ok"} over TLS
   curl -v  https://your.host.example/health 2>&1 | grep -i 'SSL\|HTTP/'   # cert chain, no -k
   # SSE must stream frames, not block until close:
   curl -N https://your.host.example/api/events?token=YOUR_TOKEN

And confirm the plaintext port is **not** reachable off the box when bound to
loopback:

.. code-block:: bash

   curl --max-time 3 http://<box-ip>:8080/health        # should refuse / time out

Hardening
=========

For a deployment reachable beyond your own machine, three opt-in settings (all off
by default) reduce the blast radius:

.. code-block:: yaml

   settings:
     ui_rate_limit: 10        # max requests/sec per client on /api/* (0 = off)
     ui_readonly: false       # true = block POST/PUT/PATCH/DELETE on /api/*
     ui_trusted_proxy: true   # trust X-Forwarded-For for the rate-limit client key

- ``ui_rate_limit`` token-buckets ``/api/*`` per client; over budget returns ``429``
  with ``Retry-After``. Tune to taste (browsing the UI issues a handful of calls).
- ``ui_readonly`` turns the instance into a safe, view-only share: every mutating
  route (job CRUD, backfill run/cancel, stream start/stop) returns ``403`` while
  ``GET`` views keep working.
- ``ui_trusted_proxy`` decides the rate-limit client key. **Leave it off** unless the
  app is reachable *only* through a reverse proxy that overwrites ``X-Forwarded-For``
  — otherwise a direct client can forge the header and bypass the limit. With it off,
  the key is the socket peer (the proxy's address if you're behind one).

Checklist
=========

- ``ui_auth_token`` is set.
- The plaintext ``:8080`` is not published to the public internet (loopback bind +
  proxy, a tunnel, or a private tailnet only).
- The proxy sets ``X-Forwarded-Proto`` and overwrites ``X-Forwarded-For``.
- SSE (``/api/events``) streams through the proxy (buffering off).

Threat model
============

Know what this setup does and does not protect, so you pick a posture that matches
your exposure.

**Trust boundaries**

- *Localhost only* (the default ``127.0.0.1`` bind) — no remote attacker; no token
  needed. This is the safe default.
- *Private overlay* (Tailscale/WireGuard) — the tailnet provides transport encryption
  **and** device identity. Binding to it is acceptable; the token is defence-in-depth.
- *Public internet* — only behind a TLS reverse proxy. The API must never be reachable
  in plaintext.

**What the token / session protects**

- It is API authorisation for a **single shared secret** — not per-user identity, not
  multi-tenant. The browser session is an opaque, ``HttpOnly`` cookie; the raw token is
  never embedded in a served page. ``SameSite=Lax`` means a cross-site ``POST``/
  ``DELETE`` does not carry the cookie, so the mutating routes are CSRF-protected.

**What it does NOT protect**

- It is **not** a substitute for TLS — use a proxy or a private overlay for encryption.
- A shared token has no per-user revocation beyond **rotating** it (change
  ``ui_auth_token`` and restart).
- Rate-limit and session state live in-process and **reset on restart** (fine for a
  single-node daemon).

**Residual risks & mitigations**

- ``?token=`` in an SSE URL can land in proxy/access logs — browsers use the cookie
  path by default, so prefer it; reserve ``?token=`` for non-browser clients.
- Enable ``ui_trusted_proxy`` **only** behind a proxy that overwrites
  ``X-Forwarded-For`` — otherwise the rate-limit key is forgeable.
- Use ``ui_readonly: true`` for a view-only share; bound ``ui_rate_limit``; keep
  ``data_path`` under the service's ``StateDirectory`` (see :doc:`deploy`).

**Recommended postures**

.. list-table::
   :header-rows: 1
   :widths: 18 16 14 14 38

   * - Exposure
     - Bind
     - TLS
     - Token
     - Notes
   * - LAN / localhost
     - ``127.0.0.1``
     - n/a
     - optional
     - Default; nothing reachable off-box.
   * - Tailnet
     - ``0.0.0.0``
     - overlay
     - **yes**
     - Tailscale encrypts + authenticates; token is defence-in-depth.
   * - Public
     - ``127.0.0.1``
     - **proxy**
     - **yes**
     - Behind Caddy/nginx/Tunnel; set ``ui_trusted_proxy``, consider
       ``ui_readonly`` and a bounded ``ui_rate_limit``.

See also :doc:`protect-ui` (token handling), :doc:`deploy` (run it unattended), and
:doc:`sync-remote` (off-box backups).
