"""Producer protocols — 禁止包含任何网络 I/O 实现。"""

from __future__ import annotations

from typing import Any, Protocol


class ProducerBackend(Protocol):
    """可靠投递协议。"""

    async def send(
        self,
        conversation_id: str,
        payload: dict[str, Any],
    ) -> None:
        """
        将消息投递到 Kafka。

        Args:
            conversation_id: Partition Key（同一通话路由到同一分区）。
            payload: 完整消息体（由 orchestrator/converter 组装后传入）。
        """
        ...  # pragma: no cover

    async def ensure_ready(self) -> None:
        """验证 Kafka 可达。启动时调用。"""
        ...  # pragma: no cover

    async def flush(self) -> None:
        """刷新生产者缓冲区。"""
        ...  # pragma: no cover

    async def close(self) -> None:
        """关闭生产者。"""
        ...  # pragma: no cover
