"""DedupBackend protocol - abstract interface for deduplication."""

from typing import Protocol


class DedupBackend(Protocol):
    """Protocol for deduplication backends."""

    async def should_emit(
        self,
        session_id: str,
        seq_no: int,
        *,
        processing_id: str = "",
        created_at: str = "",
        **kwargs: str,
    ) -> bool:
        """Return True if this key should be emitted (not duplicate). Key built from dedup_key_parts."""
        ...

    async def cleanup_session(self, session_id: str) -> None:
        """Optional: cleanup keys for session when it ends."""
        ...
