"""Graceful shutdown: SIGTERM handler, draining, checkpoint to Redis."""

import asyncio
import signal
from typing import Callable

import structlog

log = structlog.get_logger()


class GracefulShutdown:
    """Handle SIGTERM: set draining, wait for active sessions, optionally checkpoint."""

    def __init__(self, stop_timeout: int = 120) -> None:
        self._stop_timeout = stop_timeout
        self._draining = False
        self._shutdown_event = asyncio.Event()
        self._active_sessions: set[str] = set()
        self._lock = asyncio.Lock()

    @property
    def draining(self) -> bool:
        """True when we should reject new connections."""
        return self._draining

    def register_signal(self) -> None:
        """Register SIGTERM handler."""
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(
                    sig,
                    lambda: asyncio.create_task(self._on_signal()),
                )
            except NotImplementedError:
                # Windows may not support add_signal_handler
                pass

    async def _on_signal(self) -> None:
        """On SIGTERM: set draining and signal shutdown."""
        log.info("Received shutdown signal, entering draining mode")
        self._draining = True
        self._shutdown_event.set()

    def add_session(self, session_id: str) -> None:
        """Track active session."""
        self._active_sessions.add(session_id)

    def remove_session(self, session_id: str) -> None:
        """Remove session when done."""
        self._active_sessions.discard(session_id)

    async def wait_for_shutdown(self) -> None:
        """Wait until shutdown is requested."""
        await self._shutdown_event.wait()

    async def wait_for_sessions_or_timeout(self) -> None:
        """Wait for active sessions to finish, or stop_timeout."""
        deadline = asyncio.get_event_loop().time() + self._stop_timeout
        while self._active_sessions and asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.5)
        if self._active_sessions:
            log.warning("Shutdown timeout, forcing", remaining=list(self._active_sessions))
