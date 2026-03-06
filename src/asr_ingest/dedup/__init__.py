"""Deduplication layer - Redis or in-memory backend."""

from asr_ingest.dedup.base import DedupBackend
from asr_ingest.dedup.memory_dedup import MemoryDedup
from asr_ingest.dedup.redis_dedup import RedisDedup

__all__ = ["DedupBackend", "RedisDedup", "MemoryDedup", "get_dedup_backend"]


def get_dedup_backend(
    demo_mode: bool,
    redis_url: str = "",
    dedup_key_parts: str = "session_id,processing_id,seq_no",
) -> DedupBackend:
    """Factory: return MemoryDedup when demo_mode else RedisDedup."""
    if demo_mode:
        return MemoryDedup(dedup_key_parts=dedup_key_parts)
    return RedisDedup(redis_url=redis_url, dedup_key_parts=dedup_key_parts)
