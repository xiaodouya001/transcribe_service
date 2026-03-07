"""Tests for buffer layer - RedisBuffer and RedisBufferConsumer.
Uses fakeredis for Redis (no real Redis required)."""

import json

import pytest
from fakeredis import FakeAsyncRedis

from transcription_ingest.buffer.redis_buffer import RedisBuffer
from transcription_ingest.buffer.redis_consumer import RedisBufferConsumer
from transcription_ingest.connector.base import TranscriptionEvent
from transcription_ingest.dedup import RedisDeduplication
from transcription_ingest.transform import DefaultCleaner


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
    msgs = await redis_buffer._client.xrange(redis_buffer._stream)
    assert len(msgs) >= 1
    msg_id, fields = msgs[-1]
    stored = json.loads(fields["payload"])
    assert stored == payload


@pytest.mark.asyncio
async def test_redis_buffer_push_without_maxlen(fake_redis) -> None:
    """RedisBuffer.push without maxlen uses xadd without maxlen."""
    buffer = RedisBuffer(redis_url="redis://localhost:6379/0", maxlen=None)
    buffer._client = fake_redis
    msg_id = await buffer.push({"x": 1})
    assert msg_id


@pytest.mark.asyncio
async def test_redis_buffer_close(redis_buffer: RedisBuffer) -> None:
    """RedisBuffer.close closes client."""
    await redis_buffer.close()
    assert redis_buffer._client is None


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
    assert len(received) == 2
    assert received[0]["transcript"] == "hello"
    assert received[1]["transcript"] == "hi"


@pytest.mark.asyncio
async def test_redis_buffer_consumer_skips_invalid_json(fake_redis) -> None:
    """Consumer skips message with invalid JSON payload."""
    stream = "test:buf2"
    consumer_group = "test:cg2"
    received: list[dict] = []

    class CaptureProducer:
        async def send(self, **kwargs):
            received.append(kwargs)

        async def flush(self):
            pass

    await fake_redis.xadd(stream, {"payload": "not valid json {"})
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
    assert len(received) == 0


@pytest.mark.asyncio
async def test_redis_buffer_consumer_dedup_filters_duplicate(fake_redis) -> None:
    """Consumer filters duplicate via dedup (only first transcript sent)."""
    stream = "test:buf3"
    consumer_group = "test:cg3"
    received: list[dict] = []

    class CaptureProducer:
        async def send(self, **kwargs):
            received.append(kwargs)

        async def flush(self):
            pass

    payload = {
        "success": True,
        "result": {
            "callStatus": {"sessionId": "s1"},
            "transcripts": [
                {"seqNo": 0, "transcript": "first", "role": "Agent"},
                {"seqNo": 0, "transcript": "dup", "role": "Agent"},
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
    assert len(received) == 1
    assert received[0]["transcript"] == "first"


@pytest.mark.asyncio
async def test_redis_buffer_consumer_send_failure_removes_dedup(fake_redis) -> None:
    """When producer.send fails, dedup.remove is called so retry can resend."""
    stream = "test:buf4"
    consumer_group = "test:cg4"

    class FailingProducer:
        async def send(self, **kwargs):
            raise RuntimeError("Kafka down")

        async def flush(self):
            pass

    payload = {
        "success": True,
        "result": {
            "callStatus": {"sessionId": "s1"},
            "transcripts": [{"seqNo": 0, "transcript": "x", "role": "Agent"}],
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
        producer=FailingProducer(),
    )
    consumer._client = fake_redis

    with pytest.raises(RuntimeError, match="Kafka down"):
        await consumer.consume_once()

    assert await dedup.should_emit("s1", 0) is True


@pytest.mark.asyncio
async def test_redis_buffer_consumer_send_timeout_removes_dedup(fake_redis) -> None:
    """When producer.send times out, dedup.remove is called."""
    import asyncio
    stream = "test:buf5"
    consumer_group = "test:cg5"

    class SlowProducer:
        async def send(self, **kwargs):
            await asyncio.sleep(5.0)

        async def flush(self):
            pass

    payload = {
        "success": True,
        "result": {
            "callStatus": {"sessionId": "s1"},
            "transcripts": [{"seqNo": 0, "transcript": "x", "role": "Agent"}],
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
        producer=SlowProducer(),
        send_timeout_sec=0.1,
    )
    consumer._client = fake_redis

    with pytest.raises(RuntimeError, match="Kafka 不可用"):
        await consumer.consume_once()
    assert await dedup.should_emit("s1", 0) is True


@pytest.mark.asyncio
async def test_redis_buffer_consumer_processes_pending_messages(fake_redis) -> None:
    """Consumer processes pending (unacked) messages from previous run."""
    stream = "test:buf6"
    consumer_group = "test:cg6"
    received: list[dict] = []

    class CaptureProducer:
        async def send(self, **kwargs):
            received.append(kwargs)

        async def flush(self):
            pass

    payload = {
        "success": True,
        "result": {
            "callStatus": {"sessionId": "s1"},
            "transcripts": [{"seqNo": 0, "transcript": "pending", "role": "Agent"}],
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
    assert received[0]["transcript"] == "pending"


@pytest.mark.asyncio
async def test_redis_buffer_consumer_stop_and_close(fake_redis) -> None:
    """stop sets _running False; close stops and closes client."""
    consumer = RedisBufferConsumer(
        redis_url="redis://localhost:6379/0",
        stream="x",
        consumer_group="y",
        dedup=RedisDeduplication(client=fake_redis),
        cleaner=DefaultCleaner(),
        producer=type("P", (), {"send": lambda *a, **k: None, "flush": lambda *a, **k: None})(),
    )
    consumer._client = fake_redis
    consumer._running = True
    consumer.stop()
    assert consumer._running is False
    await consumer.close()
    assert consumer._client is None


