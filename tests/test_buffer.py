"""Tests for buffer layer - RedisBuffer and RedisBufferConsumer."""

import json

import pytest
from fakeredis import FakeAsyncRedis

from asr_ingest.buffer.redis_buffer import RedisBuffer
from asr_ingest.buffer.redis_consumer import RedisBufferConsumer
from asr_ingest.connector.base import TranscriptionEvent
from asr_ingest.dedup import RedisDeduplication
from asr_ingest.transform import DefaultCleaner


@pytest.fixture
def fake_redis():
    """Fake Redis client with Stream support."""
    return FakeAsyncRedis(decode_responses=True)


@pytest.fixture
def redis_buffer(fake_redis):
    """RedisBuffer with injected fake Redis."""
    buffer = RedisBuffer(redis_url="redis://localhost:6379/0", maxlen=100)
    buffer._client = fake_redis
    return buffer


@pytest.mark.asyncio
async def test_redis_buffer_push(redis_buffer: RedisBuffer) -> None:
    """RedisBuffer.push adds payload to stream and returns message id."""
    payload = {"success": True, "result": {"transcripts": [{"seqNo": 0, "transcript": "hi"}]}}
    msg_id = await redis_buffer.push(payload)
    assert msg_id
    assert "0-" in msg_id or "-" in msg_id


@pytest.mark.asyncio
async def test_redis_buffer_push_stored_correctly(redis_buffer: RedisBuffer) -> None:
    """Pushed payload is stored as JSON in stream."""
    payload = {"foo": "bar", "nested": {"a": 1}}
    await redis_buffer.push(payload)
    # Read back from stream
    msgs = await redis_buffer._client.xrange(redis_buffer._stream)
    assert len(msgs) >= 1
    msg_id, fields = msgs[-1]
    stored = json.loads(fields["payload"])
    assert stored == payload


@pytest.mark.asyncio
async def test_redis_buffer_consumer_processes_message(fake_redis) -> None:
    """RedisBufferConsumer processes stream message: dedup, clean, send to producer."""
    stream = "test:buffer"
    consumer_group = "test:consumer"
    received: list[dict] = []

    class CaptureProducer:
        async def send(self, **kwargs):
            received.append(kwargs)

        async def flush(self):
            pass

    # Pre-populate stream with a payload
    payload = {
        "success": True,
        "result": {
            "processingId": "p1",
            "callStatus": {"sessionId": "s1"},
            "transcripts": [
                {"seqNo": 0, "transcript": "hello", "role": "Agent"},
                {"seqNo": 1, "transcript": "hi", "role": "Customer"},
            ],
        },
    }
    await fake_redis.xadd(stream, {"payload": json.dumps(payload)})
    await fake_redis.xgroup_create(stream, consumer_group, id="0", mkstream=True)

    dedup = RedisDeduplication(client=fake_redis)
    consumer = RedisBufferConsumer(
        redis_url="redis://localhost:6379/0",
        stream=stream,
        consumer_group=consumer_group,
        dedup=dedup,
        cleaner=DefaultCleaner(),
        producer=CaptureProducer(),
    )
    consumer._client = fake_redis

    n = await consumer.consume_once()
    assert n == 1
    assert len(received) == 2  # 2 transcripts
    assert received[0]["transcript"] == "hello"
    assert received[1]["transcript"] == "hi"
