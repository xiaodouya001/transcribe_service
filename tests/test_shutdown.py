"""Tests for graceful shutdown."""

import asyncio

import pytest

from transcription_ingest.shutdown.graceful import GracefulShutdown


@pytest.fixture
def shutdown():
    """GracefulShutdown with short timeout for tests."""
    return GracefulShutdown(stop_timeout=2)


def test_shutdown_draining_initially_false(shutdown: GracefulShutdown) -> None:
    """draining is False before signal."""
    assert shutdown.draining is False


def test_shutdown_add_remove_session(shutdown: GracefulShutdown) -> None:
    """add_session and remove_session track active sessions."""
    shutdown.add_session("s1")
    shutdown.add_session("s2")
    assert "s1" in shutdown._active_sessions
    assert "s2" in shutdown._active_sessions
    shutdown.remove_session("s1")
    assert "s1" not in shutdown._active_sessions
    assert "s2" in shutdown._active_sessions
    shutdown.remove_session("s2")
    assert "s2" not in shutdown._active_sessions


def test_shutdown_remove_nonexistent_session(shutdown: GracefulShutdown) -> None:
    """remove_session on non-existent session does not raise."""
    shutdown.remove_session("nonexistent")


@pytest.mark.asyncio
async def test_shutdown_wait_for_sessions_or_timeout(shutdown: GracefulShutdown) -> None:
    """wait_for_sessions_or_timeout returns when sessions empty."""
    shutdown.add_session("s1")
    task = asyncio.create_task(shutdown.wait_for_sessions_or_timeout())
    await asyncio.sleep(0.1)
    shutdown.remove_session("s1")
    await asyncio.sleep(0.2)
    await task


@pytest.mark.asyncio
async def test_shutdown_wait_for_shutdown(shutdown: GracefulShutdown) -> None:
    """wait_for_shutdown completes when _shutdown_event is set."""
    async def set_event():
        await asyncio.sleep(0.05)
        shutdown._shutdown_event.set()

    task = asyncio.create_task(shutdown.wait_for_shutdown())
    asyncio.create_task(set_event())
    await asyncio.wait_for(task, timeout=1.0)


def test_shutdown_register_signal() -> None:
    """register_signal registers handlers (mocked for cross-platform)."""
    from unittest.mock import patch
    shutdown = GracefulShutdown(stop_timeout=2)
    with patch.object(asyncio.get_event_loop(), "add_signal_handler", side_effect=NotImplementedError):
        with patch("sys.platform", "win32"):
            shutdown.register_signal()


@pytest.mark.asyncio
async def test_shutdown_on_signal() -> None:
    """_on_signal sets draining and shutdown_event."""
    shutdown = GracefulShutdown(stop_timeout=2)
    assert shutdown.draining is False
    await shutdown._on_signal()
    assert shutdown.draining is True
    assert shutdown._shutdown_event.is_set()


def test_shutdown_sync_signal_handler() -> None:
    """_sync_signal_handler sets draining and shutdown_event."""
    import signal
    shutdown = GracefulShutdown(stop_timeout=2)
    shutdown._sync_signal_handler(signal.SIGTERM, None)
    assert shutdown.draining is True
    assert shutdown._shutdown_event.is_set()


@pytest.mark.asyncio
async def test_shutdown_wait_for_sessions_timeout() -> None:
    """wait_for_sessions_or_timeout returns when sessions remain after deadline."""
    shutdown = GracefulShutdown(stop_timeout=0)
    shutdown.add_session("s1")
    await shutdown.wait_for_sessions_or_timeout()
    assert "s1" in shutdown._active_sessions
