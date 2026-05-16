#!/usr/bin/env python3
# coding: utf-8

from unittest.mock import MagicMock, patch

from dccd.daemon.config import RemoteConfig, StorageConfig
from dccd.daemon.storage import RemoteStorage


def _storage(tmp_path, remotes=None):
    cfg = StorageConfig(local_path=str(tmp_path), remotes=remotes or [])
    return RemoteStorage(cfg)


def _remote():
    return RemoteConfig(provider='rclone', remote='mynas:crypto/')


def _remotes(*paths):
    return [RemoteConfig(provider='rclone', remote=p) for p in paths]


# ---------------------------------------------------------------------------
# push() without remotes — no-op
# ---------------------------------------------------------------------------

def test_push_no_remote_never_calls_subprocess(tmp_path):
    s = _storage(tmp_path)
    sub_dir = tmp_path / 'Binance'
    sub_dir.mkdir()
    with patch('subprocess.run') as mock_run:
        s.push(sub_dir)
    mock_run.assert_not_called()


# ---------------------------------------------------------------------------
# check_rclone()
# ---------------------------------------------------------------------------

def test_check_rclone_found(tmp_path):
    s = _storage(tmp_path, [_remote()])
    with patch('shutil.which', return_value='/usr/bin/rclone'):
        assert s.check_rclone() is True


def test_check_rclone_not_found_logs_warning(tmp_path, caplog):
    s = _storage(tmp_path, [_remote()])
    import logging
    with patch('shutil.which', return_value=None):
        with caplog.at_level(logging.WARNING, logger='dccd.daemon.storage'):
            result = s.check_rclone()
    assert result is False
    assert 'rclone not found' in caplog.text


def test_check_rclone_cached(tmp_path):
    s = _storage(tmp_path, [_remote()])
    with patch('shutil.which', return_value='/usr/bin/rclone') as mock_which:
        s.check_rclone()
        s.check_rclone()
    mock_which.assert_called_once()


def test_check_rclone_no_remotes_returns_false(tmp_path):
    s = _storage(tmp_path)
    with patch('shutil.which', return_value='/usr/bin/rclone'):
        assert s.check_rclone() is False


# ---------------------------------------------------------------------------
# push() with single remote + rclone present
# ---------------------------------------------------------------------------

def test_push_calls_rclone_with_correct_args(tmp_path):
    s = _storage(tmp_path, [_remote()])
    sub_dir = tmp_path / 'Binance' / 'Data'
    sub_dir.mkdir(parents=True)

    mock_result = MagicMock()
    mock_result.returncode = 0

    with patch('shutil.which', return_value='/usr/bin/rclone'):
        with patch('subprocess.run', return_value=mock_result) as mock_run:
            s.push(sub_dir)

    mock_run.assert_called_once()
    args = mock_run.call_args[0][0]
    assert args[0] == 'rclone'
    assert args[1] == 'copy'
    assert args[2] == str(sub_dir.resolve())
    assert args[3] == 'mynas:crypto/Binance/Data'


def test_push_remote_no_trailing_slash(tmp_path):
    s = _storage(tmp_path, [RemoteConfig(provider='rclone', remote='mynas:crypto')])
    sub_dir = tmp_path / 'sub'
    sub_dir.mkdir()

    mock_result = MagicMock()
    mock_result.returncode = 0

    with patch('shutil.which', return_value='/usr/bin/rclone'):
        with patch('subprocess.run', return_value=mock_result) as mock_run:
            s.push(sub_dir)

    dest = mock_run.call_args[0][0][3]
    assert dest == 'mynas:crypto/sub'


# ---------------------------------------------------------------------------
# push() with multiple remotes
# ---------------------------------------------------------------------------

def test_push_multiple_remotes_calls_rclone_twice(tmp_path):
    s = _storage(tmp_path, _remotes('mynas:crypto/', 's3:bucket/crypto/'))
    sub_dir = tmp_path / 'sub'
    sub_dir.mkdir()

    mock_result = MagicMock()
    mock_result.returncode = 0

    with patch('shutil.which', return_value='/usr/bin/rclone'):
        with patch('subprocess.run', return_value=mock_result) as mock_run:
            s.push(sub_dir)

    assert mock_run.call_count == 2
    dests = [call[0][0][3] for call in mock_run.call_args_list]
    assert 'mynas:crypto/sub' in dests
    assert 's3:bucket/crypto/sub' in dests


# ---------------------------------------------------------------------------
# push() root path (path == local_path)
# ---------------------------------------------------------------------------

def test_push_root_path_uses_remote_root(tmp_path):
    s = _storage(tmp_path, [_remote()])

    mock_result = MagicMock()
    mock_result.returncode = 0

    with patch('shutil.which', return_value='/usr/bin/rclone'):
        with patch('subprocess.run', return_value=mock_result) as mock_run:
            s.push(tmp_path)

    dest = mock_run.call_args[0][0][3]
    assert dest == 'mynas:crypto'


# ---------------------------------------------------------------------------
# push() with rclone not found
# ---------------------------------------------------------------------------

def test_push_rclone_absent_does_not_call_subprocess(tmp_path):
    s = _storage(tmp_path, [_remote()])
    sub_dir = tmp_path / 'sub'
    sub_dir.mkdir()

    with patch('shutil.which', return_value=None):
        with patch('subprocess.run') as mock_run:
            s.push(sub_dir)

    mock_run.assert_not_called()


# ---------------------------------------------------------------------------
# push() when rclone returns non-zero exit code
# ---------------------------------------------------------------------------

def test_push_rclone_failure_logs_error_no_exception(tmp_path, caplog):
    s = _storage(tmp_path, [_remote()])
    sub_dir = tmp_path / 'sub'
    sub_dir.mkdir()

    mock_result = MagicMock()
    mock_result.returncode = 1
    mock_result.stderr = 'connection refused'

    import logging
    with patch('shutil.which', return_value='/usr/bin/rclone'):
        with patch('subprocess.run', return_value=mock_result):
            with caplog.at_level(logging.ERROR, logger='dccd.daemon.storage'):
                s.push(sub_dir)  # must NOT raise

    assert 'rclone copy failed' in caplog.text


# ---------------------------------------------------------------------------
# push() with path outside base
# ---------------------------------------------------------------------------

def test_push_path_outside_base_logs_error(tmp_path, caplog):
    s = _storage(tmp_path, [_remote()])
    outside = tmp_path.parent / 'other'
    outside.mkdir(exist_ok=True)

    import logging
    with patch('shutil.which', return_value='/usr/bin/rclone'):
        with patch('subprocess.run') as mock_run:
            with caplog.at_level(logging.ERROR, logger='dccd.daemon.storage'):
                s.push(outside)

    mock_run.assert_not_called()
    assert 'not under base' in caplog.text
