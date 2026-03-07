"""Reconnect loop with exponential backoff for long-lived connections."""

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import structlog

log = structlog.get_logger(__name__)


def _is_stt_connection_error(err: BaseException) -> bool:
    """True if error indicates STT provider service is not reachable."""
    s = str(err).lower()
    err_type = type(err).__name__.lower()
    return (
        "502" in s or "503" in s or "504" in s
        or "bad gateway" in s
        or "connection refused" in s
        or "connection" in s and ("refused" in s or "closed" in s or "拒绝" in s)
        or "httperror" in err_type
        or "httpstatuserror" in err_type
    )


def _log_connection_failure(err: BaseException, settings: Any) -> None:
    """Log with clear hint when STT provider connection fails."""
    url = getattr(settings, "stt_provider_url", "")
    if _is_stt_connection_error(err):
        log.exception(
            "Reconnect: 连接 STT 失败（STT 提供商服务未就绪，将自动重试）",
            url=url,
            error=str(err),
        )
    else:
        log.exception(
            "Reconnect: 连接 STT 失败（将自动重试）",
            url=url,
            error=str(err),
        )


async def run_with_reconnect(
    connect_fn: Callable[[str | None], Awaitable[str | None]],
    settings: Any,
    shutdown: Any = None,
) -> None:
    """Run connect_fn in a retry loop with exponential backoff.

    connect_fn(last_event_id) performs one connection attempt and returns
    last_event_id for SSE (or None for WebSocket) when the connection ends.
    """
    if not getattr(settings, "reconnect_enabled", True):
        last_id = await connect_fn(None)
        return

    max_retries = getattr(settings, "reconnect_max_retries", 0)
    initial_delay = getattr(settings, "reconnect_initial_delay", 1.0)
    max_delay = getattr(settings, "reconnect_max_delay", 60.0)
    backoff_factor = getattr(settings, "reconnect_backoff_factor", 2.0)

    last_event_id: str | None = None
    attempt = 0
    last_error: BaseException | None = None

    while True:
        if shutdown and getattr(shutdown, "draining", False):
            log.info("Reconnect: 收到关闭信号，退出重连循环")
            break

        try:
            last_event_id = await connect_fn(last_event_id)
            if last_event_id is None:
                break
        except asyncio.CancelledError:
            raise
        except Exception as e:
            last_error = e
            _log_connection_failure(e, settings)

        attempt += 1
        if max_retries > 0 and attempt >= max_retries:
            log.error("Reconnect: 已达最大重试次数", max_retries=max_retries)
            if last_error is not None:
                raise last_error
            raise RuntimeError("Max retries reached")

        delay = min(
            initial_delay * (backoff_factor ** (attempt - 1)),
            max_delay,
        )
        log.info(
            "Reconnect: 即将重连",
            attempt=attempt,
            delay_sec=round(delay, 1),
            last_event_id=last_event_id,
        )
        # Sleep in small chunks so we can exit quickly on Ctrl+C / draining
        elapsed = 0.0
        while elapsed < delay:
            if shutdown and getattr(shutdown, "draining", False):
                break
            await asyncio.sleep(min(0.5, delay - elapsed))
            elapsed += 0.5
