"""RedisBufferConsumer - consume from Stream, dedup, clean, send to Kafka."""

import asyncio
import json
from typing import Any

import structlog
from redis.asyncio import Redis

from transcription_ingest.connector.base import TranscriptionEvent

log = structlog.get_logger(__name__)


def _log_kafka_failure(msg_id: str, err: BaseException) -> None:
    """Log with clear hint. Producer.send 失败时消息未 XACK，会保留在 Buffer 自动重试。"""
    log.exception(
        "Buffer Consumer: 处理消息失败（Kafka 不可用，消息已保留在 Buffer，将自动重试）",
        msg_id=msg_id,
        error=str(err),
    )


class RedisBufferConsumer:
    """Consume from Redis Stream: XREADGROUP -> parse -> Dedup -> Cleaner -> Producer -> XACK."""

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        stream: str = "transcription:ingest:buffer",
        consumer_group: str = "transcription:ingest:consumer",
        consumer: str = "worker1",
        dedup: Any = None,
        cleaner: Any = None,
        producer: Any = None,
        *,
        send_timeout_sec: float = 10.0,
    ) -> None:
        self._redis_url = redis_url
        self._stream = stream
        self._consumer_group = consumer_group
        self._consumer = consumer
        self._dedup = dedup
        self._cleaner = cleaner
        self._producer = producer
        self._send_timeout = send_timeout_sec
        self._client: Redis | None = None
        self._running = False

    async def _get_client(self) -> Redis:
        if self._client is None:
            self._client = Redis.from_url(self._redis_url, decode_responses=True)
        return self._client

    async def _ensure_group(self) -> None:
        """Create stream and consumer group if not exist."""
        client = await self._get_client()
        try:
            await client.xgroup_create(
                self._stream,
                self._consumer_group,
                id="0",
                mkstream=True,
            )
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                raise

    async def _process_message(self, msg_id: str, payload_str: str) -> None:
        """Parse payload, expand events, dedup, clean, send, ack."""
        try:
            payload = json.loads(payload_str)
        except json.JSONDecodeError:
            return
        r = payload.get("result") or {}
        cs = r.get("callStatus") or {}
        log.info(
            "Buffer Consumer: 正在处理消息",
            msg_id=msg_id,
            session_id=cs.get("sessionId", ""),
        )
        for event in TranscriptionEvent.from_vendor_payload(payload):
            if await self._dedup.should_emit(
                event.session_id,
                event.seq_no,
                processing_id=event.processing_id,
                created_at=event.created_at,
            ):
                cleaned_result = self._cleaner.clean(payload, event)
                log.info(
                    "Buffer Consumer: 发送 transcript 到 Kafka",
                    session_id=event.session_id,
                    seq_no=event.seq_no,
                    transcript=event.transcript[:30] + "..." if len(event.transcript) > 30 else event.transcript,
                )
                try:
                    await asyncio.wait_for(
                        self._producer.send(
                            session_id=event.session_id,
                            seq_no=event.seq_no,
                            transcript=event.transcript,
                            role=event.role,
                            created_at=event.created_at,
                            processing_status=event.processing_status,
                            processing_id=event.processing_id,
                            raw_payload=cleaned_result.get("raw"),
                            cleaned=cleaned_result.get("cleaned"),
                        ),
                        timeout=self._send_timeout,
                    )
                except (asyncio.TimeoutError, Exception) as e:
                    # 发送失败时撤销 dedup 记录，以便重试时再次尝试发送
                    if hasattr(self._dedup, "remove"):
                        await self._dedup.remove(
                            event.session_id,
                            event.seq_no,
                            processing_id=event.processing_id,
                            created_at=event.created_at,
                        )
                    if isinstance(e, asyncio.TimeoutError):
                        raise RuntimeError(
                            f"Kafka 不可用：发送超时({self._send_timeout}s)，消息已保留在 Buffer"
                        ) from None
                    raise
        client = await self._get_client()
        await client.xack(self._stream, self._consumer_group, msg_id)
        await client.xdel(self._stream, msg_id)

    async def consume_once(self) -> int:
        """Process one batch. Returns number of messages processed."""
        client = await self._get_client()
        processed = 0
        # Prefer new messages first (block 200ms) so we process injects quickly
        new = await client.xreadgroup(
            self._consumer_group,
            self._consumer,
            {self._stream: ">"},
            count=10,
            block=200,
        )
        if new:
            n = sum(len(m) for _, m in new)
            log.info("Buffer Consumer: 从 Redis 收到消息", count=n)
            for _stream_name, messages in new:
                for msg_id, fields in messages:
                    payload_str = fields.get("payload") or "{}"
                    try:
                        await self._process_message(msg_id, payload_str)
                    except Exception as e:
                        _log_kafka_failure(msg_id, e)
                        raise
                    processed += 1
            return processed
        # Then pending (unacked from previous run)
        pending = await client.xreadgroup(
            self._consumer_group,
            self._consumer,
            {self._stream: "0"},
            count=10,
        )
        if pending:
            n = sum(len(m) for _, m in pending)
            if n > 0:
                log.info("Buffer Consumer: 处理未确认的旧消息", count=n)
            for _stream_name, messages in pending:
                for msg_id, fields in messages:
                    payload_str = fields.get("payload") or "{}"
                    try:
                        await self._process_message(msg_id, payload_str)
                    except Exception as e:
                        _log_kafka_failure(msg_id, e)
                        raise
                    processed += 1
        return processed

    async def consume_loop(self) -> None:
        """Run consume loop until stopped."""
        self._running = True
        await self._ensure_group()
        log.info("Buffer Consumer: 已启动", stream=self._stream, group=self._consumer_group)
        empty_polls = 0
        while self._running:
            try:
                n = await self.consume_once()
                if n == 0:
                    empty_polls += 1
                    if empty_polls % 200 == 1 and empty_polls > 1:
                        log.debug("Buffer Consumer: 空闲轮询", empty_polls=empty_polls)
                else:
                    empty_polls = 0
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.exception(
                    "Buffer Consumer: 消费循环异常（Kafka 不可用，消息已保留在 Buffer，将自动重试）",
                    error=str(e),
                )
                await asyncio.sleep(0.5)

    def stop(self) -> None:
        """Signal stop."""
        self._running = False

    async def close(self) -> None:
        """Close Redis connection."""
        self.stop()
        if self._client:
            await self._client.aclose()
            self._client = None
