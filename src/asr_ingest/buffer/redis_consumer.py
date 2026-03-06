"""RedisBufferConsumer - consume from Stream, dedup, clean, send to Kafka."""

import asyncio
import json
from typing import Any

from redis.asyncio import Redis

from asr_ingest.connector.base import TranscriptionEvent


class RedisBufferConsumer:
    """Consume from Redis Stream: XREADGROUP -> parse -> Dedup -> Cleaner -> Producer -> XACK."""

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        stream: str = "asr:ingest:buffer",
        consumer_group: str = "asr:ingest:consumer",
        consumer: str = "worker1",
        dedup: Any = None,
        cleaner: Any = None,
        producer: Any = None,
    ) -> None:
        self._redis_url = redis_url
        self._stream = stream
        self._consumer_group = consumer_group
        self._consumer = consumer
        self._dedup = dedup
        self._cleaner = cleaner
        self._producer = producer
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
        for event in TranscriptionEvent.from_vendor_payload(payload):
            if await self._dedup.should_emit(
                event.session_id,
                event.seq_no,
                processing_id=event.processing_id,
                created_at=event.created_at,
            ):
                cleaned_result = self._cleaner.clean(payload, event)
                await self._producer.send(
                    session_id=event.session_id,
                    seq_no=event.seq_no,
                    transcript=event.transcript,
                    role=event.role,
                    created_at=event.created_at,
                    processing_status=event.processing_status,
                    processing_id=event.processing_id,
                    raw_payload=cleaned_result.get("raw"),
                    cleaned=cleaned_result.get("cleaned"),
                )
        client = await self._get_client()
        await client.xack(self._stream, self._consumer_group, msg_id)

    async def consume_once(self) -> int:
        """Process one batch. Returns number of messages processed."""
        client = await self._get_client()
        processed = 0
        # First try pending (unacked from previous run)
        pending = await client.xreadgroup(
            self._consumer_group,
            self._consumer,
            {self._stream: "0"},
            count=10,
        )
        if pending:
            for _stream_name, messages in pending:
                for msg_id, fields in messages:
                    payload_str = fields.get("payload") or "{}"
                    await self._process_message(msg_id, payload_str)
                    processed += 1
            return processed
        # Then new messages
        new = await client.xreadgroup(
            self._consumer_group,
            self._consumer,
            {self._stream: ">"},
            count=10,
            block=1000,
        )
        if new:
            for _stream_name, messages in new:
                for msg_id, fields in messages:
                    payload_str = fields.get("payload") or "{}"
                    await self._process_message(msg_id, payload_str)
                    processed += 1
        return processed

    async def consume_loop(self) -> None:
        """Run consume loop until stopped."""
        self._running = True
        await self._ensure_group()
        while self._running:
            try:
                await self.consume_once()
            except asyncio.CancelledError:
                break
            except Exception:
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
