#!/usr/bin/env python3
# coding: utf-8

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dccd.tools.websocket import BasisWebSocket


def _make_ws(host: str = 'wss://example.com') -> BasisWebSocket:
    ws = BasisWebSocket.__new__(BasisWebSocket)
    BasisWebSocket.__init__(ws, host)
    return ws


@pytest.mark.asyncio
async def test_wait_that_resolves():
    ws = _make_ws()
    ws.flag = False

    async def _set_flag():
        await asyncio.sleep(0)
        ws.flag = True

    asyncio.ensure_future(_set_flag())
    await ws.wait_that('flag')
    assert ws.flag is True


@pytest.mark.asyncio
async def test_on_error_calls_on_close():
    ws = _make_ws()
    ws.on_close = MagicMock()
    await ws.on_error('test error', 'detail')
    ws.on_close.assert_called_once()


def test_on_open_retries_then_succeeds():
    ws = _make_ws()
    calls = []

    def _side_effect(*args, **kwargs):
        calls.append(1)
        if len(calls) < 3:
            raise OSError('connection refused')

    with patch('dccd.tools.websocket.asyncio.run', side_effect=_side_effect):
        ws.on_open()

    assert len(calls) == 3


def test_on_open_max_retries_raises():
    ws = _make_ws(host='wss://example.com')
    ws.max_retries = 3

    with patch('dccd.tools.websocket.asyncio.run', side_effect=OSError('fail')):
        with pytest.raises(OSError):
            ws.on_open()


def test_on_close_sets_is_connect_false():
    ws = _make_ws()
    ws.is_connect = True
    ws.ws = MagicMock()
    ws.on_close()
    assert ws.is_connect is False
