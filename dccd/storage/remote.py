"""Remote storage copy via rclone.

The remote is an *archive superset* of the local store: files are copied
off-box but never deleted remotely.  Local = hot tier (fast access, space
pressure managed by the purge subsystem); remote = complete history archive.
This means:

- A local file that is purged to free disk space still exists on the remote
  and can be pulled back by :meth:`RemoteStorage.restore` (read-through
  restore).
- Remote cleanup (removing datasets you no longer want) is a **manual**
  operation — use ``rclone delete`` or the provider console.  dccd will
  never delete from the remote automatically.
"""

from __future__ import annotations

import asyncio
import logging
import pathlib
import subprocess

__all__ = ["RemoteStorage"]

logger = logging.getLogger(__name__)


class RemoteStorage:
    """Copy local data to one or more rclone remotes (non-destructive).

    Each sync cycle runs ``rclone copy`` (not ``rclone sync``) so that files
    present on the remote but absent locally — e.g. files purged from the hot
    tier to reclaim disk — are **never deleted**.  The remote grows
    monotonically and acts as a complete off-box archive; :meth:`restore` pulls
    individual dataset directories back on demand.

    Parameters
    ----------
    local_path : str or Path
        Local data directory to copy from.
    remotes : list of dicts
        Each dict has ``provider`` and ``remote`` keys.
    """

    def __init__(
        self,
        local_path: str | pathlib.Path,
        remotes: list[dict[str, str]] | None = None,
    ) -> None:
        self._local = pathlib.Path(local_path)
        self._remotes = remotes or []

    def sync_one(self, remote: str) -> bool:
        """Copy local store to a single rclone remote.  Returns True on success.

        Uses ``rclone copy`` (not ``rclone sync``) so files that exist on the
        remote but are absent locally are preserved.  This guarantees that
        files purged from the local hot tier remain available on the remote for
        read-through :meth:`restore`.
        """
        try:
            result = subprocess.run(
                ["rclone", "copy", str(self._local), remote, "--quiet"],
                capture_output=True,
                text=True,
                timeout=300,
            )
            if result.returncode != 0:
                logger.error("rclone copy to %s failed: %s", remote, result.stderr)
                return False
            logger.info("Copied to %s", remote)
            return True
        except FileNotFoundError:
            logger.error("rclone not found in PATH")
            return False
        except subprocess.TimeoutExpired:
            logger.error("rclone copy to %s timed out", remote)
            return False

    def restore(self, rel_path: str) -> bool:
        """Pull *rel_path* back from the first configured remote into the store.

        Runs ``rclone copy {remote}/{rel_path} {local}/{rel_path}`` — **copy**,
        not sync, so nothing is ever deleted. Used for read-through restore when a
        dataset's local Parquet was purged but still exists off-box. Returns True
        on success (or no-op when no remote is configured).

        Parameters
        ----------
        rel_path : str
            Dataset directory relative to the local store root.
        """
        if not self._remotes:
            return False
        remote = self._remotes[0].get("remote", "")
        if not remote:
            return False
        src = remote.rstrip("/") + "/" + rel_path
        dst = self._local / rel_path
        try:
            dst.mkdir(parents=True, exist_ok=True)
            result = subprocess.run(
                ["rclone", "copy", src, str(dst), "--quiet"],
                capture_output=True,
                text=True,
                timeout=300,
            )
            if result.returncode != 0:
                logger.error("rclone restore of %s failed: %s", rel_path, result.stderr)
                return False
            logger.info("Restored %s from %s", rel_path, remote)
            return True
        except FileNotFoundError:
            logger.error("rclone not found in PATH")
            return False
        except subprocess.TimeoutExpired:
            logger.error("rclone restore of %s timed out", rel_path)
            return False

    async def sync_all(self) -> dict[str, bool]:
        """Sync to all configured remotes concurrently."""
        if not self._remotes:
            return {}

        loop = asyncio.get_running_loop()
        results: dict[str, bool] = {}
        tasks = []
        for r in self._remotes:
            remote = r.get("remote", "")
            if remote:
                task = loop.run_in_executor(None, self.sync_one, remote)
                tasks.append((remote, task))

        for remote, task in tasks:
            results[remote] = await task

        return results
