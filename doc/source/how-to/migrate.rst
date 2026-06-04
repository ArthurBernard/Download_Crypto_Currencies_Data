=========================
Migrate data from dccd v2
=========================

Upgrade legacy Parquet files to the v3 schema (back up first):

.. code-block:: bash

   dccd migrate --dry-run       # preview
   dccd migrate --no-dry-run    # apply (idempotent, never drops rows)
