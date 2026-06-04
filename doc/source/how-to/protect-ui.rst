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
