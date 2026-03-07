"""Entry point: 转录接入与分发流程（Connector -> Dedup -> Producer）。"""

import asyncio
import sys
import time
from pathlib import Path

# Add project root for config import
_project_root = Path(__file__).resolve().parents[2]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from config.logging_config import configure_logging, get_logger
from config.settings import get_settings
from transcription_ingest.connector import get_connector
from transcription_ingest.connector.reconnect import run_with_reconnect
from transcription_ingest.dedup import get_dedup_backend
from transcription_ingest.producer import get_producer_backend
from transcription_ingest.shutdown.graceful import GracefulShutdown
from transcription_ingest.transform import get_cleaner

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
        log.error("Transcription Ingest: 启动失败（Redis 不可用）", redis_url=redis_url, error=str(e))
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
        log.error("Transcription Ingest: 启动失败（Kafka 不可用）", error=str(e))
        raise RuntimeError(f"Kafka 不可用: {e}") from e


async def run_ingest(redis_buffer_enabled: bool | None = None) -> None:
    """运行转录接入与分发流程：Connector -> Dedup -> Producer。

    redis_buffer_enabled: 指定时覆盖配置，控制是否启用 Redis Buffer 模式。
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
        "Transcription Ingest: 已启动",
        mode=settings.mode,
        redis_buffer=use_buffer,
        reconnect_enabled=reconnect_enabled,
    )

    async def connect_fn(last_event_id: str | None) -> str | None:
        connector = get_connector(settings, last_event_id)
        try:
            if use_buffer:
                from transcription_ingest.buffer import RedisBuffer, RedisBufferConsumer

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
                    block_ms=getattr(settings, "redis_buffer_block_ms", 50),
                )
                connector_task = asyncio.create_task(connector.connect_and_push(buffer))
                consumer_task = asyncio.create_task(consumer.consume_loop())
                shutdown_waiter = asyncio.create_task(shutdown._shutdown_event.wait())
                try:
                    # 先完成者胜出：Connector 自然结束，或收到 Ctrl+C 立即断开
                    done, pending = await asyncio.wait(
                        [connector_task, shutdown_waiter],
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if shutdown_waiter in done:
                        connector_task.cancel()
                        try:
                            await connector_task
                        except asyncio.CancelledError:
                            pass
                        log.info("Transcription Ingest: 已断开 STT 连接（优雅停机）")
                    else:
                        shutdown_waiter.cancel()
                        try:
                            await shutdown_waiter
                        except asyncio.CancelledError:
                            pass
                        await connector_task  # 可能抛连接异常
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
                    received_at = payload.pop("_ingest_received_at", None)
                    if await dedup.should_emit(
                        event.session_id,
                        event.seq_no,
                        processing_id=event.processing_id,
                        created_at=event.created_at,
                    ):
                        cleaned = cleaner.clean(payload, event)
                        log.info(
                            "Transcription Ingest: 发送 transcript 到 Kafka",
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
                        if received_at is not None:
                            log.debug(
                                "Transcription Ingest: STT 收到到发送 Kafka 耗时",
                                duration_ms=round((time.monotonic() - received_at) * 1000),
                                session_id=event.session_id,
                                seq_no=event.seq_no,
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
        log.exception("Transcription Ingest: 运行异常", error=str(e))
        raise
    finally:
        log.info("Transcription Ingest: 正在关闭连接")
        await producer.flush()
        if hasattr(producer, "close"):
            fn = producer.close
            if asyncio.iscoroutinefunction(fn):
                await fn()
            else:
                fn()
        if hasattr(dedup, "close"):
            await dedup.close()
        log.info("Transcription Ingest: 已安全退出")


def run() -> None:
    """启动转录接入与分发服务。"""
    asyncio.run(run_ingest())


if __name__ == "__main__":
    run()
