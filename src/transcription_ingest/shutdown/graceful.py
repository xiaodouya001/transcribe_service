"""Graceful shutdown: SIGTERM handler, draining, checkpoint to Redis."""

import asyncio
import signal
from typing import Callable

import structlog

log = structlog.get_logger(__name__)


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
        """Register SIGTERM/SIGINT handler. Uses signal.signal on Windows (add_signal_handler unsupported)."""
        import sys
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(
                    sig,
                    lambda: asyncio.create_task(self._on_signal()),
                )
            except NotImplementedError:
                # Windows: add_signal_handler not supported, use signal.signal
                if sys.platform == "win32":
                    signal.signal(sig, self._sync_signal_handler)

    def _sync_signal_handler(self, signum: int, frame: object) -> None:
        """Sync handler for Windows. Sets draining; 2nd Ctrl+C raises KeyboardInterrupt."""
        log.info("Shutdown: 收到终止信号")
        self._draining = True
        self._shutdown_event.set()
        # Restore default so 2nd Ctrl+C forces exit
        signal.signal(signum, signal.SIG_DFL)

    async def _on_signal(self) -> None:
        """On SIGTERM: set draining and signal shutdown."""
        log.info("Shutdown: 收到终止信号")
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
            log.warning("Shutdown: 超时强制退出", remaining=list(self._active_sessions))
