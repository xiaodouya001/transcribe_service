"""Backend protocols and shared enums for Redis-backed sequencing and ownership guard."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Protocol


class PrepareResult(str, Enum):
    """Lua pre-check return values for sequence state machine."""

    PRE_CHECK_OK = "PRE_CHECK_OK"
    IDEMPOTENT = "IDEMPOTENT"
    OUT_OF_ORDER = "OUT_OF_ORDER"


@dataclass(frozen=True)
class PrepareOutcome:
    """Prepare result plus optional sequencing metadata for logging/diagnostics."""

    status: PrepareResult
    expected_sequence: int | None = None


class SequenceStateMachineBackend(Protocol):
    """Distributed sequence guard protocol."""

    async def prepare(self, conversation_id: str, seq: int) -> PrepareOutcome:
        ...  # pragma: no cover

    async def commit(self, conversation_id: str, seq: int) -> None:
        ...  # pragma: no cover

    async def cleanup(self, conversation_id: str) -> None:
        ...  # pragma: no cover

    async def close(self) -> None:
        ...  # pragma: no cover


class ConversationOwnershipGuardBackend(Protocol):
    """Cross-connection conversation send ownership guard protocol."""

    async def claim_or_refresh(self, conversation_id: str, ownership_token: str) -> bool:
        ...  # pragma: no cover

    async def release(self, conversation_id: str, ownership_token: str) -> None:
        ...  # pragma: no cover

    async def close(self) -> None:
        ...  # pragma: no cover
