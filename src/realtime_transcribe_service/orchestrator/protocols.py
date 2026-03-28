"""Orchestrator protocols — do not import concrete implementations from ``impl/``."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass
class OrchestratorResult:
    """Unified transport-facing result returned by the orchestrator.

    Attributes:
        response: JSON dictionary sent to the client (ACK or ERROR).
        disconnect: Whether the WebSocket should be closed.
        close_code: WebSocket close code to use when disconnecting.
        timings_ms: Stage timings in milliseconds, used only for troubleshooting.
    """

    response: dict
    disconnect: bool = False
    close_code: int = 1000
    timings_ms: dict[str, float] | None = None


class OrchestratorBackend(Protocol):
    """Business orchestration protocol."""

    async def handle_message(
        self,
        raw_json: object,
        conversation_id: str = "",
    ) -> OrchestratorResult:
        """
        Handle one inbound message and execute the full 2PC flow.

        Returns:
            An ``OrchestratorResult`` containing the response frame, disconnect flag,
            and close code.
        """
        ...  # pragma: no cover
