"""Orchestrator protocols — 禁止 import 任何 impl/ 下的具体实现。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass
class OrchestratorResult:
    """调度层返回给 transport 的统一结果。

    Attributes:
        response: 发送给客户端的 JSON dict（ACK 或 ERROR）。
        disconnect: 是否应断开 WebSocket。
        close_code: 断开时使用的 WebSocket Close Code。
        timings_ms: 仅用于排障的分段耗时（毫秒）。
    """

    response: dict
    disconnect: bool = False
    close_code: int = 1000
    timings_ms: dict[str, float] | None = None


class OrchestratorBackend(Protocol):
    """业务编排协议。"""

    async def handle_message(self, raw_json: dict) -> OrchestratorResult:
        """
        处理一条上行消息，执行完整的 2PC 流程。

        Returns:
            OrchestratorResult 包含响应帧、是否断连、Close Code。
        """
        ...  # pragma: no cover
