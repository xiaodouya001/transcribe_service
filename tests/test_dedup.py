"""Tests for dedup layer."""

import pytest
from asr_ingest.dedup import MemoryDedup, get_dedup_backend


@pytest.mark.asyncio
async def test_memory_dedup_first_emit() -> None:
    """First call for (session, seq_no) should return True."""
    dedup = MemoryDedup()
    assert await dedup.should_emit("s1", 0) is True


@pytest.mark.asyncio
async def test_memory_dedup_duplicate_filtered() -> None:
    """Second call for same (session, seq_no) should return False."""
    dedup = MemoryDedup()
    assert await dedup.should_emit("s1", 0) is True
    assert await dedup.should_emit("s1", 0) is False


@pytest.mark.asyncio
async def test_memory_dedup_different_seq_no() -> None:
    """Different seq_no for same session should both emit."""
    dedup = MemoryDedup()
    assert await dedup.should_emit("s1", 0) is True
    assert await dedup.should_emit("s1", 1) is True


@pytest.mark.asyncio
async def test_memory_dedup_different_sessions() -> None:
    """Same seq_no in different sessions should both emit."""
    dedup = MemoryDedup()
    assert await dedup.should_emit("s1", 0) is True
    assert await dedup.should_emit("s2", 0) is True


@pytest.mark.asyncio
async def test_get_dedup_backend_demo() -> None:
    """Demo mode returns MemoryDedup."""
    backend = get_dedup_backend(demo_mode=True)
    assert isinstance(backend, MemoryDedup)
    assert await backend.should_emit("s1", 0) is True
    assert await backend.should_emit("s1", 0) is False


@pytest.mark.asyncio
async def test_get_dedup_backend_prod() -> None:
    """Non-demo returns RedisDedup (we don't test Redis without infra)."""
    from asr_ingest.dedup import RedisDedup

    backend = get_dedup_backend(demo_mode=False, redis_url="redis://localhost:6379/0")
    assert isinstance(backend, RedisDedup)
