"""Application entrypoint — lifecycle wiring and dependency injection only."""

import asyncio
import sys
import time
from collections.abc import Awaitable
from typing import Any, cast

import uvicorn
from pydantic import ValidationError

from realtime_transcribe_service.service_runtime import (
    RuntimeBundle,
    close_runtime_bundle,
    create_runtime_bundle,
)
from realtime_transcribe_service.config.logging_config import (
    configure_logging,
    get_logger,
    redact_redis_url_for_logs,
    redact_text_for_logs,
)
from realtime_transcribe_service.config.settings import Settings, get_settings
from realtime_transcribe_service.producer.kafka_producer import KafkaProducer
from realtime_transcribe_service.schemas.error_codes import WsCloseCode
from realtime_transcribe_service.constants import (
    DEFAULT_HTTP_HOST,
    WS_CLOSE_REASON_GOING_AWAY,
    WS_PATH,
)
from realtime_transcribe_service.redis.runtime import create_shared_redis_client

log = get_logger(__name__)


async def _check_redis(settings: Settings, *, client: Any | None = None) -> None:
    """Verify Redis connectivity."""
    redis_url = settings.redis_url
    assert redis_url is not None
    owns_client = client is None
    client = create_shared_redis_client(settings) if client is None else client
    assert client is not None
    redacted_url = redact_redis_url_for_logs(redis_url)
    try:
        await cast(Awaitable[Any], client.ping())
    except Exception as e:
        err_safe = redact_text_for_logs(
            str(e), extra_secret=settings.redis_password
        )
        log.error(
            "Startup failed: Redis unavailable",
            redis_url=redacted_url,
            error=err_safe,
        )
        raise RuntimeError(
            f"Redis unavailable: {redacted_url} - {err_safe}"
        ) from e
    finally:
        if owns_client:
            assert client is not None
            await client.aclose()


async def _startup_phase_timed(phase: str, coro: Awaitable[Any]) -> None:
    """Run one startup check and always log its duration to pinpoint slow phases."""
    t0 = time.perf_counter()
    try:
        await coro
    finally:
        log.info(
            "Startup: Phase completed",
            phase=phase,
            elapsed_ms=round((time.perf_counter() - t0) * 1000, 2),
        )


async def _graceful_stop(
    server: uvicorn.Server,
    server_task: asyncio.Task[None],
    registry: Any,
    producer: KafkaProducer,
) -> None:
    """Run the graceful shutdown main flow in the required order."""
    await registry.close_all(
        code=WsCloseCode.GOING_AWAY, reason=WS_CLOSE_REASON_GOING_AWAY
    )
    await producer.flush()
    server.should_exit = True
    await server_task


async def _safe_serve(server: uvicorn.Server) -> None:
    """Run uvicorn and normalize startup failures into RuntimeError."""
    try:
        await server.serve()
    except SystemExit as e:
        raise RuntimeError(f"Uvicorn failed to start (exit code {e.code})") from e


async def _check_kafka(producer: KafkaProducer, timeout: float) -> None:
    """Verify Kafka connectivity."""
    try:
        await asyncio.wait_for(
            producer.ensure_ready(),
            timeout=timeout,
        )
    except asyncio.TimeoutError:
        log.error("Startup failed: Kafka connection timed out", timeout_sec=timeout)
        try:
            await producer.close()
        except Exception as close_exc:
            log.warning(
                "Kafka: Failed to close producer after timeout",
                error=repr(close_exc),
                exc_type=type(close_exc).__name__,
                exc_info=True,
            )
        raise RuntimeError(f"Kafka unavailable: connection timed out after {timeout}s") from None
    except Exception as e:
        log.exception(
            "Startup failed: Kafka unavailable",
            error=repr(e),
            exc_type=type(e).__name__,
        )
        raise RuntimeError(f"Kafka unavailable: {e}") from e


async def run() -> None:
    """Start Realtime Transcribe Service."""
    settings = get_settings()
    configure_logging(level=settings.log_level, format=settings.log_format)

    redis_url = settings.redis_url
    kafka_bootstrap_servers = settings.kafka_bootstrap_servers
    assert redis_url is not None
    assert kafka_bootstrap_servers is not None

    runtime: RuntimeBundle | None = None
    try:
        runtime = await create_runtime_bundle(settings)

        # --- Pre-start checks (Redis and Kafka run in parallel to reduce cold-start latency) ---
        t_checks = time.perf_counter()
        await asyncio.gather(
            _startup_phase_timed(
                "redis",
                _check_redis(settings, client=runtime.shared_redis_client),
            ),
            _startup_phase_timed(
                "kafka",
                _check_kafka(runtime.producer, settings.kafka_startup_timeout_sec),
            ),
        )
        log.info(
            "Startup: Redis+Kafka checks completed (parallel)",
            wall_ms=round((time.perf_counter() - t_checks) * 1000, 2),
        )

        log.info(
            "Realtime Transcribe Service: Started",
            ws_endpoint=WS_PATH,
            host=DEFAULT_HTTP_HOST,
            port=settings.http_port,
        )

        server_task = asyncio.create_task(_safe_serve(runtime.server))
        shutdown_task = asyncio.create_task(runtime.shutdown.wait_for_shutdown())

        done, _ = await asyncio.wait(
            [server_task, shutdown_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        if server_task in done:
            shutdown_task.cancel()
            server_task.result()

        # --- Graceful shutdown ---
        log.info(
            "Shutdown: Starting graceful shutdown",
            timeout_sec=runtime.shutdown.stop_timeout,
        )
        try:
            await asyncio.wait_for(
                _graceful_stop(
                    runtime.server,
                    server_task,
                    runtime.registry,
                    runtime.producer,
                ),
                timeout=runtime.shutdown.stop_timeout,
            )
        except asyncio.TimeoutError:
            log.warning(
                "Shutdown: Graceful shutdown timed out, forcing final cleanup",
                timeout_sec=runtime.shutdown.stop_timeout,
            )
            runtime.server.should_exit = True
            if not server_task.done():
                server_task.cancel()
                try:
                    await server_task
                except asyncio.CancelledError:
                    pass
    except Exception as e:
        log.exception("Runtime failure", error=str(e))
        raise
    finally:
        log.info("Shutdown: Releasing resources")
        if runtime is not None:
            await close_runtime_bundle(runtime)
        log.info("Realtime Transcribe Service: Exited cleanly")


def main() -> None:
    """Synchronous entrypoint."""
    try:
        asyncio.run(run())
    except ValidationError as e:
        sys.stderr.write(f"Configuration invalid:\n{e}\n")
        sys.exit(1)
    except RuntimeError as e:
        log.error("Startup failed", error=str(e))
        sys.exit(1)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":  # pragma: no cover
    main()

