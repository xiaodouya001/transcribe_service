"""Graceful shutdown — signal handling, drain state, and shutdown coordination."""

from __future__ import annotations

import asyncio
import signal
import sys

from realtime_transcribe_service.config.logging_config import get_logger

log = get_logger(__name__)


class GracefulShutdown:
    """Handle SIGTERM/SIGINT, mark drain mode, and notify the main loop to exit."""

    def __init__(self, stop_timeout: float = 120.0) -> None:
        self._stop_timeout = stop_timeout
        self._draining = False
        self._shutdown_event = asyncio.Event()

    @property
    def stop_timeout(self) -> float:
        """Total graceful shutdown budget in seconds."""
        return self._stop_timeout

    @property
    def draining(self) -> bool:
        """Whether new connections should now be rejected."""
        return self._draining

    def register_signal(self) -> None:
        """Register SIGTERM/SIGINT handlers."""
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self._on_signal()))
            except NotImplementedError:
                if sys.platform == "win32":
                    signal.signal(sig, self._sync_signal_handler)

    def _sync_signal_handler(self, signum: int, frame: object) -> None:
        log.info("Shutdown: Termination signal received")
        self._draining = True
        self._shutdown_event.set()
        signal.signal(signum, signal.SIG_DFL)

    async def _on_signal(self) -> None:
        log.info("Shutdown: Termination signal received")
        self._draining = True
        self._shutdown_event.set()

    async def wait_for_shutdown(self) -> None:
        """Block until a shutdown signal is received."""
        await self._shutdown_event.wait()
