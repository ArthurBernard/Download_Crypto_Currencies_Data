============
Installation
============

Requirements
------------

- Python **3.10** or later
- No API key required — all REST and WebSocket endpoints used by ``dccd`` are public.

Optional system dependency: `rclone <https://rclone.org/>`_ is needed only if you
use the daemon's remote-sync feature (``storage.remotes`` in the YAML config).

Install with pip
----------------

Core package (historical REST downloads + WebSocket streams):

.. code-block:: bash

   pip install dccd

With daemon extras (CLI scheduler, APScheduler, YAML config, rclone sync):

.. code-block:: bash

   pip install dccd[daemon]

Extras summary
~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 20 30 50

   * - Extra
     - Command
     - Adds
   * - *(core)*
     - ``pip install dccd``
     - ``histo_dl``, ``continuous_dl``, Polars I/O
   * - ``daemon``
     - ``pip install dccd[daemon]``
     - CLI (``dccd`` command), APScheduler, PyYAML, Typer, rclone sync
   * - ``ui``
     - ``pip install dccd[daemon,ui]``
     - Web UI / JSON API (``dccd ui``): FastAPI, uvicorn, Jinja2
   * - ``dev``
     - ``pip install dccd[dev]``
     - pytest, ruff, mypy, interrogate — for contributors only

Install from source
-------------------

.. code-block:: bash

   git clone https://github.com/ArthurBernard/Download_Crypto_Currencies_Data.git
   cd Download_Crypto_Currencies_Data
   pip install -e ".[daemon,dev]"

Verify the installation
-----------------------

.. code-block:: python

   import dccd
   print(dccd.__version__)

Or via the CLI (daemon extra required):

.. code-block:: bash

   dccd --version

Shell auto-completion
---------------------

The ``dccd`` CLI supports tab-completion for shells via
`Typer <https://typer.tiangolo.com/>`_.  Run once to install:

.. code-block:: bash

   # Bash
   dccd --install-completion bash

   # Zsh
   dccd --install-completion zsh

   # Fish
   dccd --install-completion fish

After installing, restart your shell (or ``source ~/.bashrc`` / ``source ~/.zshrc``).
Auto-completion covers subcommand names, ``--exchange`` values, and ``--pairs`` values.
