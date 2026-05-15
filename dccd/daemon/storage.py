#!/usr/bin/env python3
# coding: utf-8

""" Remote storage abstraction for the dccd daemon.

Wraps rclone to push local data directories to any rclone-supported
destination (NAS, SFTP, S3, Google Drive, …).

"""

from __future__ import annotations

import logging
import pathlib
import shutil
import subprocess
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dccd.daemon.config import StorageConfig

__all__ = ['RemoteStorage']

logger = logging.getLogger(__name__)


class RemoteStorage:
    """ Push local data directories to a remote destination via rclone.

    If no remote is configured, :meth:`push` is a silent no-op and data
    is kept on the daemon host only.

    Parameters
    ----------
    config : StorageConfig
        Storage configuration (local path + optional rclone remote).

    """

    def __init__(self, config: StorageConfig) -> None:
        self.config = config
        self._rclone_available: bool | None = None

    def check_rclone(self) -> bool:
        """ Check whether rclone is available in PATH (result is cached).

        Returns
        -------
        bool
            ``True`` if rclone is found, ``False`` otherwise.

        """
        if self._rclone_available is None:
            self._rclone_available = shutil.which('rclone') is not None
            if not self._rclone_available and self.config.remote is not None:
                logger.warning(
                    'rclone not found in PATH — remote sync disabled. '
                    'Install rclone and ensure it is on your PATH.'
                )
        return self._rclone_available

    def push(self, local_path: str | pathlib.Path) -> None:
        """ Copy *local_path* to the configured remote destination.

        The remote target mirrors the relative directory structure under
        ``config.local_path``.  For example, if ``local_path`` is
        ``/data/crypto/Binance/Data/Clean_Data/Hourly/BTCUSDT`` and
        ``config.local_path`` is ``/data/crypto``, the remote target will
        be ``<remote>/Binance/Data/Clean_Data/Hourly/BTCUSDT``.

        Parameters
        ----------
        local_path : str or pathlib.Path
            Absolute path of the directory to synchronise.

        Notes
        -----
        This method never raises — failures are logged as errors so that
        a remote-push failure does not interrupt the local collection loop.

        """
        if self.config.remote is None:
            return

        if not self.check_rclone():
            return

        local_abs = pathlib.Path(local_path).resolve()
        base_abs = pathlib.Path(self.config.local_path).resolve()

        try:
            rel = local_abs.relative_to(base_abs)
        except ValueError:
            logger.error(
                'push() called with path %s that is not under base %s',
                local_abs, base_abs,
            )
            return

        remote_target = self.config.remote.remote.rstrip('/') + '/' + str(rel)

        result = subprocess.run(
            ['rclone', 'copy', str(local_abs), remote_target],
            capture_output=True,
            text=True,
            timeout=300,
        )

        if result.returncode != 0:
            logger.error(
                'rclone copy failed (%s → %s): %s',
                local_abs, remote_target, result.stderr.strip(),
            )
