"""E2E 集成测试 - Transcribe Service 去重验证。"""

import pytest
from transcribe_service.connector.base import TranscriptionEvent
from transcribe_service.dedup import RedisDeduplication


@pytest.fixture
def fake_redis_dedup():
    """RedisDeduplication with fakeredis for unit tests."""
    from fakeredis import FakeAsyncRedis
    client = FakeAsyncRedis(decode_responses=True)
    return RedisDeduplication(client=client)


@pytest.mark.asyncio
async def test_ingest_dedup_filters_duplicates(fake_redis_dedup: RedisDeduplication) -> None:
    """Transcribe Service 应通过 dedup 过滤重复的 (session_id, seq_no)。"""
    dedup = fake_redis_dedup
    received: list[dict] = []

    class CaptureProducer:
        async def send(self, **kwargs):
            received.append(kwargs)

        async def flush(self):
            pass

    # Simulate events: e0, e0 (dup), e1, e0 again (dup)
    events = [
        TranscriptionEvent("s1", 0, "hello", "Agent"),
        TranscriptionEvent("s1", 0, "hello", "Agent"),  # duplicate
        TranscriptionEvent("s1", 1, "hi", "Customer"),
    ]

    producer = CaptureProducer()
    for event in events:
        if await dedup.should_emit(event.session_id, event.seq_no):
            await producer.send(
                session_id=event.session_id,
                seq_no=event.seq_no,
                transcript=event.transcript,
                role=event.role,
            )

    assert len(received) == 2  # Only first e0 and e1
    assert received[0]["seq_no"] == 0
    assert received[1]["seq_no"] == 1


