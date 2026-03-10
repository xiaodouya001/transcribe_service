"""Tests for graceful shutdown."""

import asyncio

import pytest

from transcribe_service.shutdown.graceful import GracefulShutdown


@pytest.fixture
def shutdown():
    """GracefulShutdown with short timeout for tests."""
    return GracefulShutdown(stop_timeout=2)


def test_shutdown_draining_initially_false(shutdown: GracefulShutdown) -> None:
    """draining is False before signal."""
    assert shutdown.draining is False


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
