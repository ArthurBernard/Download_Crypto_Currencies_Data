==================================
Sync data to a remote (S3, GCS, …)
==================================

dccd can mirror your local Parquet store to off-box storage so a server loss
doesn't lose data — and, optionally, **drop old local files** once they're safely
copied, pulling them back on demand. The whole chain is built on
`rclone <https://rclone.org/>`_.

Provision a remote
==================

Install rclone and create a remote interactively:

.. code-block:: bash

   rclone config            # follow the prompts; name it e.g. "s3"
   rclone listremotes       # -> s3:

This writes ``~/.config/rclone/rclone.conf``. In a container, mount or inject
that file (it holds credentials — keep it out of the image):

.. code-block:: bash

   docker run -v $HOME/.config/rclone:/root/.config/rclone:ro ... dccd start

Configure sync
==============

.. code-block:: yaml

   storage:
     remotes:
       - {provider: rclone, remote: "s3:my-bucket/crypto"}
     sync_interval: 3600        # seconds between sync cycles
     min_free_gb: 0             # >0 enables the free-space purge (see below)

With at least one remote configured, ``dccd start`` runs a periodic loop that
mirrors the store every ``sync_interval`` seconds (one-way ``rclone sync`` —
remote becomes a mirror of local). Each cycle is recorded as a ``sync`` run, so
the **Storage** page shows the last/next sync, status and synced volume, with a
**Sync now** button (``POST /api/storage/sync``). On failure the loop backs off
exponentially.

Free up disk: purge + restore
=============================

Set ``min_free_gb`` above ``0`` to let the daemon reclaim space. After **each
successful sync** (so the data is already off-box), if free space on the store's
filesystem is below the floor, dccd deletes the **oldest** Parquet files until it
is back above it. The ``.dccd`` directory (run history + coverage manifest) is
never touched.

Two mechanisms make this safe:

- **Backfill still resumes correctly.** A coverage manifest under ``.dccd``
  records each dataset's extent, so ``start="last"`` resumes from where collection
  left off even when the local files are gone — no accidental re-download.
- **Reads pull data back.** Reading a dataset whose local files were purged
  triggers a **read-through restore** (``rclone copy`` of that dataset directory
  back into the store) before loading — transparent to ``Client.read`` and
  ``POST /api/read``.

Restore and integrity
======================

``rclone sync`` is a one-way mirror (local → remote); the remote is a faithful
copy, deduplicated by dccd before upload. To rehydrate a fresh machine, pull the
whole tree back:

.. code-block:: bash

   rclone copy s3:my-bucket/crypto /path/to/data/crypto

To spot-check integrity, compare counts/sizes between local and remote:

.. code-block:: bash

   rclone check /path/to/data/crypto s3:my-bucket/crypto

Because the coverage manifest lives under ``.dccd`` (also synced), a restored
instance keeps its resume cursors and continues collecting without gaps.
