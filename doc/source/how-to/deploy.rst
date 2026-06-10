===========================================
Deploy dccd on a server (run it unattended)
===========================================

Run ``dccd start`` (scheduler + streams + web UI) 24/7 on a server, surviving
crashes and reboots. Two blessed paths are below; **systemd is recommended** for a
long-lived home/VPS box (no container overhead, journald integration, clean
restart semantics). Use **Docker** for a containerised or ephemeral host.

Both were verified end to end on a real Ubuntu 24.04 server.

.. note::

   **Old CPUs without AVX2** (common on recycled home servers): the default
   ``polars`` wheel crashes the daemon at import with SIGILL. Check with
   ``grep -o -m1 avx2 /proc/cpuinfo`` — if it prints nothing, use the
   ``polars-lts-cpu`` variant (shown in both paths below).

Recommended: systemd
====================

Run from a virtualenv as a dedicated ``dccd`` user. ``deploy/dccd.service`` ships
with the repo.

.. code-block:: bash

   # 1. Service user + venv
   sudo useradd --system --no-create-home --shell /usr/sbin/nologin dccd
   sudo python3 -m venv /opt/dccd/venv
   sudo /opt/dccd/venv/bin/pip install "dccd[daemon]"
   # CPU without AVX2 only:
   #   sudo /opt/dccd/venv/bin/pip uninstall -y polars && \
   #   sudo /opt/dccd/venv/bin/pip install polars-lts-cpu

   # 2. Config at /etc/dccd/config.yml (XDG_CONFIG_HOME=/etc), readable by the
   #    service group only. data_path must be under /var/lib/dccd.
   sudo install -d -o root -g dccd -m 750 /etc/dccd
   sudo install -o root -g dccd -m 640 config.yml /etc/dccd/config.yml

   # 3. Install + enable the unit
   sudo cp deploy/dccd.service /etc/systemd/system/dccd.service
   sudo systemctl daemon-reload
   sudo systemctl enable --now dccd

The unit runs ``ExecStart=/opt/dccd/venv/bin/dccd start`` as ``User=dccd`` with
``StateDirectory=dccd`` (systemd creates and owns ``/var/lib/dccd``) and a hardened
sandbox (``ProtectSystem=strict``, ``ProtectHome``, ``PrivateTmp``,
``NoNewPrivileges``). ``Restart=on-failure`` brings it back after a crash, and
because it is ``enable``\ d it auto-starts after a reboot — verified by a real
``systemctl reboot``: the daemon came back, the stream reconnected and the interval
backfills re-armed with no data gap.

Check it:

.. code-block:: bash

   systemctl status dccd
   curl -fsS http://127.0.0.1:8080/health        # {"status":"ok"}
   journalctl -u dccd -f                          # live logs

Operate it
==========

- **Health**: ``GET /health`` returns 200 when up. The systemd unit restarts on
  failure; the Docker image ships a ``HEALTHCHECK`` so an orchestrator sees
  ``healthy``/``unhealthy``.
- **Logs & rotation**: dccd writes no log files — logs go to the journal
  (``journalctl -u dccd``). Rotation is journald's job (``/etc/systemd/journald.conf``:
  ``SystemMaxUse=``, ``MaxRetentionSec=``). Under Docker, the container engine owns
  rotation (``--log-opt max-size=…``).
- **Alerts**: set ``alerts.webhook_url`` (and ``alerts.max_consecutive_errors``) to
  POST a webhook when a job fails repeatedly. Verified: a failing job past the
  threshold delivers the alert.
- **Resource limits**: the unit ships commented ``MemoryMax``/``CPUQuota``/``TasksMax``
  — uncomment and tune for your host (too-low values can OOM-kill a busy daemon).
- **Secrets**: keep the token out of the image/VCS — see :doc:`protect-ui`.
- **Off-box backups**: mirror the store with rclone — see :doc:`sync-remote`.

Alternative: Docker
===================

.. code-block:: bash

   # Build (add the build-arg only for a no-AVX2 host):
   docker build -t dccd .
   #   docker build --build-arg POLARS_VARIANT=polars-lts-cpu -t dccd .

   # Run: mount the config (kept out of the image) + a named data volume.
   docker run -d --name dccd --restart unless-stopped -p 8080:8080 \
       -v "$PWD/config.yml:/etc/dccd/config.yml:ro" \
       -v dccd-data:/data \
       dccd

The image is built on a digest-pinned ``python:3.12-slim``, sets
``XDG_CONFIG_HOME=/etc`` (config at ``/etc/dccd/config.yml``), exposes ``8080``, and
declares a ``HEALTHCHECK`` against ``/health``. Set ``settings.data_path: /data`` and
``settings.ui_host: 0.0.0.0`` in the mounted config. Check ``docker inspect
--format '{{.State.Health.Status}}' dccd`` → ``healthy``.

Reaching the UI from another machine
====================================

The default bind is ``127.0.0.1``. To reach the UI from a laptop or phone, put it
behind a private network (e.g. Tailscale) or a TLS reverse proxy — never expose the
API plaintext on the public internet. The full procedure (Caddy, nginx, Cloudflare
Tunnel, Tailscale) is in :doc:`expose-remote`.
