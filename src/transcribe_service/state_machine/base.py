"""StateMachineBackend 抽象接口 — 禁止包含任何 I/O 实现。"""

from __future__ import annotations

from enum import Enum
from typing import Protocol


class PrepareResult(str, Enum):
    """Lua 预检返回值。"""

    PRE_CHECK_OK = "PRE_CHECK_OK"
    IDEMPOTENT = "IDEMPOTENT"
    OUT_OF_ORDER = "OUT_OF_ORDER"


class StateMachineBackend(Protocol):
    """分布式序列守卫协议。"""

    async def prepare(self, conversation_id: str, seq: int) -> PrepareResult:
        """
        原子预检。

        - seq == expected → PRE_CHECK_OK（不 INCR）
        - seq < expected  → IDEMPOTENT
        - seq > expected  → OUT_OF_ORDER
        """
        ...  # pragma: no cover

    async def commit(self, conversation_id: str, seq: int) -> None:
        """Kafka Ack 后推进 expected_seq = seq + 1，并续租 TTL。"""
        ...  # pragma: no cover

    async def cleanup(self, conversation_id: str) -> None:
        """SESSION_COMPLETE 后主动清理 Key（缩短 TTL 或 DEL）。"""
        ...  # pragma: no cover

    async def close(self) -> None:
        """释放连接资源。"""
        ...  # pragma: no cover
