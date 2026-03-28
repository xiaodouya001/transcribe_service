"""Producer protocols — this module must not contain any network I/O implementation."""

from __future__ import annotations

from typing import Any, Protocol


class ProducerBackend(Protocol):
    """Reliable delivery protocol."""

    async def send(
        self,
        conversation_id: str,
        payload: dict[str, Any],
    ) -> None:
        """
        Deliver a message to Kafka.

        Args:
            conversation_id: Partition key so one call stays on one partition.
            payload: Full outbound message assembled by the orchestrator and converter.
        """
        ...  # pragma: no cover

    async def ensure_ready(self) -> None:
        """Verify Kafka connectivity. Called during startup."""
        ...  # pragma: no cover

    async def flush(self) -> None:
        """Flush producer buffers."""
        ...  # pragma: no cover

    async def close(self) -> None:
        """Close the producer."""
        ...  # pragma: no cover
