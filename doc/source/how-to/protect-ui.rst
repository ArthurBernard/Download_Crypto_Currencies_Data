===============================
Protect the web UI with a token
===============================

Set a token; the API then requires ``Authorization: Bearer <token>`` and the UI
injects it automatically:

.. code-block:: yaml

   settings:
     ui_auth_token: "a-long-random-string"

Keep the default ``127.0.0.1`` bind for anything sensitive, or front it with a
reverse proxy. See :doc:`/http-api` and :doc:`/web-ui`.

Injecting the token at deploy time
==================================

The config file holds the token, so **keep it out of the image and out of version
control** — inject it at run time. dccd reads ``config.yml`` from
``$XDG_CONFIG_HOME/dccd/config.yml`` (the image sets ``XDG_CONFIG_HOME=/etc``); the
``Dockerfile`` copies only the code, never a config, so nothing secret is baked in
(verify with ``docker history`` / ``docker run --rm --entrypoint sh dccd -c 'ls
/etc/dccd'`` — empty unless mounted).

The YAML loader does **not** expand ``${ENV}`` placeholders in values, so the
blessed pattern is a **mounted config file** kept outside the repo:

.. code-block:: bash

   # Docker: mount a config that lives outside the image/VCS
   docker run -v $PWD/config.yml:/etc/dccd/config.yml:ro -v dccd-data:/data dccd
   # plus, only if storage.remotes is set, the rclone credentials:
   #   -v $HOME/.config/rclone:/root/.config/rclone:ro

.. code-block:: bash

   # systemd: keep the config readable only by root + the service group
   sudo install -o root -g dccd -m 640 config.yml /etc/dccd/config.yml

Never commit a real token; the shipped ``examples/config.example.yml`` carries only
a ``null`` placeholder.
