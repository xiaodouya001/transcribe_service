"""Entry point: Transcribe Service Webhook 模式，ConnectorManager 多会话."""

import asyncio
import sys
from pathlib import Path

# Add project root for config import
_project_root = Path(__file__).resolve().parents[2]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from config.logging_config import configure_logging, get_logger
from config.settings import get_settings
from transcription_ingest.dedup import get_dedup_backend
from transcription_ingest.producer import get_producer_backend
from transcription_ingest.shutdown.graceful import GracefulShutdown
from transcription_ingest.transform import get_cleaner
from transcription_ingest.webhook import create_app
from transcription_ingest.connector.manager import ConnectorManager

import uvicorn

log = get_logger(__name__)


async def _check_redis(redis_url: str) -> None:
    """Verify Redis is reachable. Raises with clear error on failure."""
    from redis.asyncio import Redis

    client = Redis.from_url(redis_url, decode_responses=True)
    try:
        await client.ping()
    except Exception as e:
        log.error("Transcribe Service: 启动失败（Redis 不可用）", redis_url=redis_url, error=str(e))
        raise RuntimeError(f"Redis 不可用: {redis_url} - {e}") from e
    finally:
        await client.aclose()


async def _check_kafka(producer) -> None:
    """Verify Kafka is reachable. Raises with clear error on failure."""
    if not hasattr(producer, "ensure_ready"):
        return
    try:
        await asyncio.wait_for(producer.ensure_ready(), timeout=30.0)
    except asyncio.TimeoutError:
        log.error("Transcribe Service: 启动失败（Kafka 连接超时 30s，请确认 docker compose 已启动且 Kafka 就绪）")
        raise RuntimeError("Kafka 不可用: 连接超时 30s") from None
    except Exception as e:
        log.error("Transcribe Service: 启动失败（Kafka 不可用）", error=str(e))
        raise RuntimeError(f"Kafka 不可用: {e}") from e


async def run_webhook_mode() -> None:
    """运行 Webhook 模式：FastAPI + Uvicorn，接收 Vendor Webhook，ConnectorManager 建连。"""
    settings = get_settings()
    configure_logging(level=settings.log_level, format=settings.log_format)

    dedup = get_dedup_backend(
        redis_url=settings.redis_url,
        dedup_key_parts=settings.dedup_key_parts,
        dedup_ttl_seconds=settings.dedup_ttl_seconds,
    )
    producer = get_producer_backend(
        kafka_bootstrap=settings.kafka_bootstrap_servers,
        kafka_topic=settings.kafka_topic,
        compression_type=getattr(settings, "kafka_compression_type", "none"),
        send_timeout_sec=settings.kafka_send_timeout_sec,
    )
    cleaner = get_cleaner(getattr(settings, "cleaner_mode", "default"))

    shutdown = GracefulShutdown(stop_timeout=settings.stop_timeout)
    shutdown.register_signal()

    await _check_redis(settings.redis_url)
    await _check_kafka(producer)

    connector_manager = ConnectorManager(
        dedup=dedup,
        cleaner=cleaner,
        producer=producer,
        settings=settings,
        shutdown=shutdown,
    )

    app = create_app(
        connector_manager,
        redis_url=settings.redis_url,
        producer=producer,
    )

    config = uvicorn.Config(app, host="0.0.0.0", port=8080)
    server = uvicorn.Server(config)

    log.info(
        "Transcribe Service: 已启动（Webhook 模式）",
        webhook_path="/webhook/session",
        port=8080,
        docs_url="http://127.0.0.1:8080/docs",
    )

    try:
        server_task = asyncio.create_task(server.serve())
        await shutdown.wait_for_shutdown()
        log.info("Transcribe Service: 收到关闭信号，等待活跃会话…")
        await connector_manager.wait_for_sessions(timeout=float(settings.stop_timeout))
        server.should_exit = True
        await server_task
    except Exception as e:
        log.exception("Transcribe Service: 运行异常", error=str(e))
        raise
    finally:
        log.info("Transcribe Service: 正在关闭连接")
        await producer.flush()
        if hasattr(producer, "close"):
            fn = producer.close
            if asyncio.iscoroutinefunction(fn):
                await fn()
            else:
                fn()
        if hasattr(dedup, "close"):
            await dedup.close()
        log.info("Transcribe Service: 已安全退出")


def run() -> None:
    """启动 Transcribe Service（Webhook 模式）。"""
    asyncio.run(run_webhook_mode())


if __name__ == "__main__":
    run()
