"""Entry point: connector -> dedup -> producer pipeline."""

import asyncio
import os
import sys
from pathlib import Path

# Add project root for config import
_project_root = Path(__file__).resolve().parents[2]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import structlog

from config.settings import get_settings
from asr_ingest.connector.sse import SSEConnector
from asr_ingest.connector.websocket import WebSocketConnector
from asr_ingest.dedup import get_dedup_backend
from asr_ingest.producer import get_producer_backend
from asr_ingest.transform import get_cleaner

structlog.configure(
    processors=[
        structlog.processors.add_log_level,
        structlog.dev.ConsoleRenderer(),
    ]
)
log = structlog.get_logger()


def _use_buffer_mode(settings) -> bool:
    """Buffer mode when enabled and not demo."""
    if settings.demo_mode:
        return False
    return getattr(settings, "redis_buffer_enabled", False)


async def run_pipeline() -> None:
    """Run connector -> dedup -> producer pipeline."""
    settings = get_settings()
    dedup = get_dedup_backend(
        demo_mode=settings.demo_mode,
        redis_url=settings.redis_url,
        dedup_key_parts=settings.dedup_key_parts,
    )
    producer = get_producer_backend(
        demo_mode=settings.demo_mode,
        kafka_bootstrap=settings.kafka_bootstrap_servers,
        kafka_topic=settings.kafka_topic,
        demo_output_file=os.environ.get("DEMO_OUTPUT_FILE"),
    )
    cleaner = get_cleaner(getattr(settings, "cleaner_mode", "default"))

    if settings.mode == "sse":
        connector = SSEConnector(settings.fanolab_url)
    else:
        connector = WebSocketConnector(settings.fanolab_url)

    use_buffer = _use_buffer_mode(settings)
    log.info(
        "Pipeline starting",
        mode=settings.mode,
        demo_mode=settings.demo_mode,
        redis_buffer=use_buffer,
    )

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
            )
            connector_task = asyncio.create_task(connector.connect_and_push(buffer))
            consumer_task = asyncio.create_task(consumer.consume_loop())
            try:
                await connector_task
            except Exception as e:
                log.exception("Connector error", error=str(e))
                raise
            finally:
                consumer.stop()
                consumer_task.cancel()
                try:
                    await consumer_task
                except asyncio.CancelledError:
                    pass
                # Drain remaining messages
                for _ in range(10):
                    n = await consumer.consume_once()
                    if n == 0:
                        break
                await consumer.close()
                await buffer.close()
        else:
            async for event, payload in connector.connect():
                if await dedup.should_emit(
                    event.session_id,
                    event.seq_no,
                    processing_id=event.processing_id,
                    created_at=event.created_at,
                ):
                    cleaned = cleaner.clean(payload, event)
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
                    log.debug("Dedup filtered", session_id=event.session_id, seq_no=event.seq_no)
    except Exception as e:
        err_msg = str(e)
        if settings.demo_mode and "localhost" in settings.fanolab_url and "502" in err_msg:
            log.error(
                "Connection failed. For demo without real Fanolab, run: python -m asr_ingest.demo.run_e2e",
                error=err_msg,
            )
        else:
            log.exception("Pipeline error", error=err_msg)
        raise
    finally:
        await producer.flush()
        if hasattr(producer, "close"):
            producer.close()
        if hasattr(dedup, "close"):
            await dedup.close()


def run() -> None:
    """Run the async pipeline."""
    asyncio.run(run_pipeline())


if __name__ == "__main__":
    run()
