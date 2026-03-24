"""coverage: shutdown.graceful"""

from __future__ import annotations

import asyncio
import signal
import sys
from unittest.mock import MagicMock, patch

import pytest

from transcribe_service.shutdown.graceful import GracefulShutdown


@pytest.mark.asyncio
async def test_wait_for_shutdown_unblocks_on_signal():
    gs = GracefulShutdown()

    async def fire():
        await asyncio.sleep(0.02)
        await gs._on_signal()

    t = asyncio.create_task(fire())
    try:
        await asyncio.wait_for(gs.wait_for_shutdown(), timeout=2.0)
    finally:
        await asyncio.wait([t], timeout=1.0)
    assert gs.draining is True


@pytest.mark.asyncio
async def test_on_signal_sets_draining():
    gs = GracefulShutdown()
    assert gs.draining is False
    await gs._on_signal()
    assert gs.draining is True
    assert gs._shutdown_event.is_set()


def test_sync_signal_handler():
    gs = GracefulShutdown()
    gs._sync_signal_handler(signal.SIGINT, None)
    assert gs.draining is True
    assert gs._shutdown_event.is_set()


def test_register_signal_windows_fallback_when_add_handler_fails(monkeypatch):
    gs = GracefulShutdown()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        def raise_ni(*_a, **_k):
            raise NotImplementedError

        monkeypatch.setattr(loop, "add_signal_handler", raise_ni)
        with patch.object(sys, "platform", "win32"), patch("signal.signal") as sig_mock:
            gs.register_signal()
        assert sig_mock.call_count >= 1
    finally:
        asyncio.set_event_loop(None)
        loop.close()


def test_register_signal_skips_when_not_win32_and_no_handler(monkeypatch):
    """Unix: add_signal_handler raises → 非 win32 时不注册 sync handler。"""
    gs = GracefulShutdown()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        monkeypatch.setattr(loop, "add_signal_handler", MagicMock(side_effect=NotImplementedError))
        with patch.object(sys, "platform", "linux"):
            gs.register_signal()  # should not raise
    finally:
        asyncio.set_event_loop(None)
        loop.close()
