"""优雅停机 — SIGTERM 信号处理、Drain 标记、连接追踪。"""

from __future__ import annotations

import asyncio
import signal
import sys

import structlog

log = structlog.get_logger(__name__)


class GracefulShutdown:
    """处理 SIGTERM/SIGINT，标记 Drain 状态，通知主循环退出。"""

    def __init__(self, stop_timeout: int = 120) -> None:
        self._stop_timeout = stop_timeout
        self._draining = False
        self._shutdown_event = asyncio.Event()

    @property
    def stop_timeout(self) -> int:
        """优雅停机总预算（秒）。"""
        return self._stop_timeout

    @property
    def draining(self) -> bool:
        """True 时应拒绝新连接。"""
        return self._draining

    def register_signal(self) -> None:
        """注册 SIGTERM/SIGINT 信号处理器。"""
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self._on_signal()))
            except NotImplementedError:
                if sys.platform == "win32":
                    signal.signal(sig, self._sync_signal_handler)

    def _sync_signal_handler(self, signum: int, frame: object) -> None:
        log.info("Shutdown: 收到终止信号")
        self._draining = True
        self._shutdown_event.set()
        signal.signal(signum, signal.SIG_DFL)

    async def _on_signal(self) -> None:
        log.info("Shutdown: 收到终止信号")
        self._draining = True
        self._shutdown_event.set()

    async def wait_for_shutdown(self) -> None:
        """阻塞直到收到关闭信号。"""
        await self._shutdown_event.wait()
