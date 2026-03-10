"""Graceful shutdown: SIGTERM handler, draining."""

import asyncio
import signal

import structlog

log = structlog.get_logger(__name__)


class GracefulShutdown:
    """Handle SIGTERM: set draining, signal shutdown."""

    def __init__(self, stop_timeout: int = 120) -> None:
        self._stop_timeout = stop_timeout
        self._draining = False
        self._shutdown_event = asyncio.Event()

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

    async def wait_for_shutdown(self) -> None:
        """Wait until shutdown is requested."""
        await self._shutdown_event.wait()
