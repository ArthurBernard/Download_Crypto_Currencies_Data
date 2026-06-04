==================================
Sync data to a remote (S3, GCS, …)
==================================

Configure an `rclone <https://rclone.org/>`_ remote and a sync interval; the
daemon pushes after each cycle:

.. code-block:: yaml

   storage:
     remotes:
       - {provider: rclone, remote: "s3:my-bucket/crypto"}
     sync_interval: 3600
