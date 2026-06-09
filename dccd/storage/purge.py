"""Free-space purge: drop the oldest already-synced Parquet files under disk
pressure.

**Safety contract**: this deletes local data that is recoverable only from the
remote, so it must be called **only when the remote mirror is up to date** — in
practice, right after a successful :func:`~dccd.application.operations.sync_remote`
cycle. The coverage manifest (``CoverageStore``) preserves the resume cursor, so a
later ``backfill(start="last")`` still continues from where collection left off;
reads of purged ranges return what's local until read-through restore pulls them
back.

The ``.dccd`` directory (runs DB, coverage manifest) is never touched.
"""

from __future__ import annotations

import logging
import pathlib
import shutil
from typing import Any, Callable

__all__ = ["purge_to_free_space", "free_bytes"]

logger = logging.getLogger(__name__)

_GB = 1024 ** 3


def free_bytes(path: str | pathlib.Path) -> int:
    """Return free bytes on the filesystem holding *path*."""
    return shutil.disk_usage(str(path)).free


def purge_to_free_space(
    data_path: str | pathlib.Path,
    min_free_gb: float,
    *,
    dry_run: bool = False,
    free_fn: Callable[[], int] | None = None,
) -> dict[str, Any]:
    """Delete oldest Parquet files until free space reaches ``min_free_gb``.

    Files are removed oldest-first (by mtime), so recent data stays local while
    old data — already mirrored off-box — is dropped. The ``.dccd`` directory is
    excluded. No-op when ``min_free_gb <= 0`` or free space is already above the
    threshold.

    Parameters
    ----------
    data_path : str or Path
        Root of the local store.
    min_free_gb : float
        Target free space in GiB. ``<= 0`` disables purging.
    dry_run : bool, default False
        Report what would be removed without deleting.
    free_fn : callable or None
        Override for the *initial* free-bytes probe (testing). Defaults to a real
        ``shutil.disk_usage`` of *data_path*. Freeing is then simulated by adding
        each removed file's size, so the result is deterministic.

    Returns
    -------
    dict
        ``{'freed_bytes', 'removed', 'free_after'}`` — ``removed`` is the list of
        deleted file paths (oldest first).
    """
    root = pathlib.Path(data_path)
    start_free = (free_fn or (lambda: free_bytes(root)))()
    result: dict[str, Any] = {
        "freed_bytes": 0, "removed": [], "free_after": start_free,
    }
    if min_free_gb <= 0:
        return result

    target = int(min_free_gb * _GB)
    if start_free >= target:
        return result

    files = [f for f in root.rglob("*.parquet") if ".dccd" not in f.parts]
    files.sort(key=lambda f: f.stat().st_mtime)  # oldest first

    freed = 0
    removed: list[str] = []
    for f in files:
        if start_free + freed >= target:
            break
        size = f.stat().st_size
        if not dry_run:
            try:
                f.unlink()
            except OSError as exc:
                logger.warning("Could not purge %s: %s", f, exc)
                continue
        freed += size
        removed.append(str(f))

    result["freed_bytes"] = freed
    result["removed"] = removed
    result["free_after"] = start_free + freed
    if removed:
        logger.info(
            "Purged %d file(s), freed ~%.2f GiB (free now %.2f GiB)",
            len(removed), freed / _GB, result["free_after"] / _GB,
        )
    return result
