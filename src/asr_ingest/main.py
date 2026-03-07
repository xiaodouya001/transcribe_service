"""Entry point: connector -> dedup -> producer pipeline."""

import asyncio
import sys
from pathlib import Path

# Add project root for config import
_project_root = Path(__file__).resolve().parents[2]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from config.logging_config import configure_logging, get_logger
from config.settings import get_settings
from asr_ingest.connector.reconnect import run_with_reconnect
from asr_ingest.connector.sse import SseConnector
from asr_ingest.connector.websocket import WebSocketConnector
from asr_ingest.dedup import get_dedup_backend
from asr_ingest.producer import get_producer_backend
from asr_ingest.shutdown.graceful import GracefulShutdown
from asr_ingest.transform import get_cleaner

log = get_logger(__name__)


def _use_buffer_mode(settings) -> bool:
    """Buffer mode when enabled."""
    return getattr(settings, "redis_buffer_enabled", False)


async def _check_redis(redis_url: str) -> None:
    """Verify Redis is reachable. Raises with clear error on failure."""
    from redis.asyncio import Redis

    client = Redis.from_url(redis_url, decode_responses=True)
    try:
        await client.ping()
    except Exception as e:
        log.error("Pipeline: 启动失败（Redis 不可用）", redis_url=redis_url, error=str(e))
        raise RuntimeError(f"Redis 不可用: {redis_url} - {e}") from e
    finally:
        await client.aclose()


async def _check_kafka(producer) -> None:
    """Verify Kafka is reachable. Raises with clear error on failure."""
    if not hasattr(producer, "ensure_ready"):
        return
    try:
        await producer.ensure_ready()
    except Exception as e:
        log.error("Pipeline: 启动失败（Kafka 不可用）", error=str(e))
        raise RuntimeError(f"Kafka 不可用: {e}") from e


def _create_connector(settings, last_event_id: str | None):
    """Create connector with settings (ping, timeout, last_event_id)."""
    if settings.mode == "sse":
        return SseConnector(
            settings.fanolab_url,
            last_event_id,
            read_timeout=getattr(settings, "sse_read_timeout", None),
        )
    return WebSocketConnector(
        settings.fanolab_url,
        ping_interval=getattr(settings, "ws_ping_interval", 20.0),
        ping_timeout=getattr(settings, "ws_ping_timeout", 20.0),
    )


async def run_pipeline(redis_buffer_enabled: bool | None = None) -> None:
    """Run connector -> dedup -> producer pipeline.

    redis_buffer_enabled: when provided, overrides settings for buffer mode.
    """
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
    )
    cleaner = get_cleaner(getattr(settings, "cleaner_mode", "default"))
    use_buffer = (
        redis_buffer_enabled
        if redis_buffer_enabled is not None
        else _use_buffer_mode(settings)
    )

    shutdown = GracefulShutdown(stop_timeout=settings.stop_timeout)
    shutdown.register_signal()

    reconnect_enabled = getattr(settings, "reconnect_enabled", True)

    # 启动前校验 Redis、Kafka 可用，失败则立即退出并输出明确错误
    await _check_redis(settings.redis_url)
    await _check_kafka(producer)

    log.info(
        "Pipeline: 已启动",
        mode=settings.mode,
        redis_buffer=use_buffer,
        reconnect_enabled=reconnect_enabled,
    )

    async def connect_fn(last_event_id: str | None) -> str | None:
        connector = _create_connector(settings, last_event_id)
        try:
            if use_buffer:
                from asr_ingest.buffer import RedisBuffer, RedisBufferConsumer

                buffer = RedisBuffer(
                    redis_url=settings.redis_url,
                    stream=settings.redis_buffer_stream,
                    maxlen=settings.redis_buffer_maxlen,
                )
                consumer = RedisBufferConsumer(
                    redis_url=settings.redis_url,
                    stream=settings.redis_buffer_stream,
                    consumer_group=settings.redis_buffer_consumer_group,
                    dedup=dedup,
                    cleaner=cleaner,
                    producer=producer,
                    send_timeout_sec=settings.kafka_send_timeout_sec,
                )
                connector_task = asyncio.create_task(connector.connect_and_push(buffer))
                consumer_task = asyncio.create_task(consumer.consume_loop())
                try:
                    await connector_task
                    await asyncio.sleep(0.5)  # Allow consumer to process pushed messages
                except Exception as e:
                    log.exception("Connector: 连接 SSE 失败", error=str(e))
                    raise
                finally:
                    consumer.stop()
                    consumer_task.cancel()
                    try:
                        await consumer_task
                    except asyncio.CancelledError:
                        pass
                    for _ in range(10):
                        n = await consumer.consume_once()
                        if n == 0:
                            break
                    await producer.flush()
                    await consumer.close()
                    await buffer.close()
            else:
                async for event, payload in connector.connect():
                    if shutdown.draining:
                        break
                    if await dedup.should_emit(
                        event.session_id,
                        event.seq_no,
                        processing_id=event.processing_id,
                        created_at=event.created_at,
                    ):
                        cleaned = cleaner.clean(payload, event)
                        log.info(
                            "Pipeline: 发送 transcript 到 Kafka",
                            session_id=event.session_id,
                            seq_no=event.seq_no,
                            transcript=event.transcript[:30] + "..." if len(event.transcript) > 30 else event.transcript,
                        )
                        await producer.send(
                            session_id=event.session_id,
                            seq_no=event.seq_no,
                            transcript=event.transcript,
                            role=event.role,
                            created_at=event.created_at,
                            processing_status=event.processing_status,
                            processing_id=event.processing_id,
                            raw_payload=cleaned.get("raw"),
                            cleaned=cleaned.get("cleaned"),
                        )
                    else:
                        log.info("Dedup: 已过滤重复", session_id=event.session_id, seq_no=event.seq_no)
        except Exception as e:
            raise
        return getattr(connector, "last_event_id", None)

    try:
        if reconnect_enabled:
            await run_with_reconnect(connect_fn, settings, shutdown)
        else:
            await connect_fn(None)
    except Exception as e:
        log.exception("Pipeline: 运行异常", error=str(e))
        raise
    finally:
        log.info("Pipeline: 正在关闭连接")
        await producer.flush()
        if hasattr(producer, "close"):
            fn = producer.close
            if asyncio.iscoroutinefunction(fn):
                await fn()
            else:
                fn()
        if hasattr(dedup, "close"):
            await dedup.close()
        log.info("Pipeline: 已安全退出")


def run() -> None:
    """Run the async pipeline."""
    asyncio.run(run_pipeline())


if __name__ == "__main__":
    run()
