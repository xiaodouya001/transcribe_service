"""ProducerBackend protocol - abstract interface for message production."""

from typing import Any, Protocol


class ProducerBackend(Protocol):
    """Protocol for producer backends."""

    async def send(
        self,
        session_id: str,
        seq_no: int,
        transcript: str,
        role: str = "",
        created_at: str = "",
        processing_status: str = "",
        *,
        raw_payload: dict | None = None,
        cleaned: dict | None = None,
        **kwargs: Any,
    ) -> None:
        """Send a transcription event. When raw_payload/cleaned provided, write raw+cleaned format."""
        ...

    async def flush(self) -> None:
        """Flush buffered messages."""
        ...
