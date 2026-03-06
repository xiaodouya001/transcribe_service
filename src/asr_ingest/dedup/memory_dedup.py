"""In-memory deduplication for Demo mode - no Redis required."""

import asyncio
import time

from asr_ingest.dedup.base import DedupBackend
from asr_ingest.dedup.redis_dedup import _build_key

DEDUP_TTL_SECONDS = 10


class MemoryDedup:
    """In-memory dedup: dict of key -> expiry_time. Background task cleans expired keys."""

    def __init__(
        self,
        dedup_key_parts: str = "session_id,processing_id,seq_no",
    ) -> None:
        self._dedup_key_parts = dedup_key_parts
        self._store: dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._cleanup_task: asyncio.Task | None = None

    def _key(
        self,
        session_id: str,
        seq_no: int,
        *,
        processing_id: str = "",
        created_at: str = "",
        **kwargs: str,
    ) -> str:
        return _build_key(
            self._dedup_key_parts,
            session_id,
            seq_no,
            processing_id=processing_id,
            created_at=created_at,
            **kwargs,
        )

    def _is_expired(self, expiry: float) -> bool:
        return time.monotonic() >= expiry

    async def _start_cleanup(self) -> None:
        if self._cleanup_task is not None:
            return

        async def cleanup_loop() -> None:
            while True:
                await asyncio.sleep(1)
                now = time.monotonic()
                async with self._lock:
                    expired = [k for k, v in self._store.items() if v <= now]
                    for k in expired:
                        del self._store[k]

        self._cleanup_task = asyncio.create_task(cleanup_loop())

    async def should_emit(
        self,
        session_id: str,
        seq_no: int,
        *,
        processing_id: str = "",
        created_at: str = "",
        **kwargs: str,
    ) -> bool:
        """Simulate SETNX: return True if first time, False if duplicate."""
        await self._start_cleanup()
        key = self._key(
            session_id,
            seq_no,
            processing_id=processing_id,
            created_at=created_at,
            **kwargs,
        )
        expiry = time.monotonic() + DEDUP_TTL_SECONDS
        async with self._lock:
            if key in self._store and not self._is_expired(self._store[key]):
                return False
            self._store[key] = expiry
            return True

    async def cleanup_session(self, session_id: str) -> None:
        """Remove keys for session."""
        prefix = f"dedup:{session_id}:"
        async with self._lock:
            to_del = [k for k in self._store if k.startswith(prefix)]
            for k in to_del:
                del self._store[k]
