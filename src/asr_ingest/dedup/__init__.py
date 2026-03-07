"""Deduplication layer - Redis backend."""

from asr_ingest.dedup.base import DeduplicationBackend
from asr_ingest.dedup.redis_dedup import RedisDeduplication

__all__ = ["DeduplicationBackend", "RedisDeduplication", "get_dedup_backend"]


def get_dedup_backend(
    redis_url: str = "",
    dedup_key_parts: str = "session_id,processing_id,seq_no",
    dedup_ttl_seconds: int = 60,
) -> DeduplicationBackend:
    """Factory: return RedisDeduplication."""
    return RedisDeduplication(
        redis_url=redis_url,
        dedup_key_parts=dedup_key_parts,
        dedup_ttl_seconds=dedup_ttl_seconds,
    )
