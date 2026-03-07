"""Tests for dedup layer."""

import pytest
from asr_ingest.dedup import RedisDeduplication, get_dedup_backend


@pytest.fixture
def fake_redis_client():
    """Fake Redis client for unit tests (no real Redis required)."""
    from fakeredis import FakeAsyncRedis

    return FakeAsyncRedis(decode_responses=True)


@pytest.fixture
def redis_dedup(fake_redis_client):
    """RedisDeduplication with injected fake Redis."""
    return RedisDeduplication(client=fake_redis_client)


@pytest.mark.asyncio
async def test_redis_dedup_first_emit(redis_dedup: RedisDeduplication) -> None:
    """First call for (session, seq_no) should return True."""
    assert await redis_dedup.should_emit("s1", 0) is True


@pytest.mark.asyncio
async def test_redis_dedup_duplicate_filtered(redis_dedup: RedisDeduplication) -> None:
    """Second call for same (session, seq_no) should return False."""
    assert await redis_dedup.should_emit("s1", 0) is True
    assert await redis_dedup.should_emit("s1", 0) is False


@pytest.mark.asyncio
async def test_redis_dedup_different_seq_no(redis_dedup: RedisDeduplication) -> None:
    """Different seq_no for same session should both emit."""
    assert await redis_dedup.should_emit("s1", 0) is True
    assert await redis_dedup.should_emit("s1", 1) is True


@pytest.mark.asyncio
async def test_redis_dedup_different_sessions(redis_dedup: RedisDeduplication) -> None:
    """Same seq_no in different sessions should both emit."""
    assert await redis_dedup.should_emit("s1", 0) is True
    assert await redis_dedup.should_emit("s2", 0) is True


@pytest.mark.asyncio
async def test_get_dedup_backend() -> None:
    """get_dedup_backend returns RedisDeduplication."""
    backend = get_dedup_backend(redis_url="redis://localhost:6379/0")
    assert isinstance(backend, RedisDeduplication)


@pytest.mark.asyncio
async def test_redis_dedup_cleanup_session(redis_dedup: RedisDeduplication) -> None:
    """cleanup_session removes all keys for the session."""
    assert await redis_dedup.should_emit("s1", 0) is True
    assert await redis_dedup.should_emit("s1", 1) is True
    await redis_dedup.cleanup_session("s1")
    # After cleanup, same keys should emit again (keys were deleted)
    assert await redis_dedup.should_emit("s1", 0) is True
    assert await redis_dedup.should_emit("s1", 1) is True


def test_dedup_build_key_format() -> None:
    """_build_key produces correct key format from config."""
    from asr_ingest.dedup.redis_dedup import _build_key

    key = _build_key("session_id,processing_id,seq_no", "s1", 0, processing_id="p1")
    assert key == "dedup:s1:p1:0"
    key2 = _build_key("session_id,seq_no", "s2", 1)
    assert key2 == "dedup:s2:1"
