"""Conversation owner backend abstraction."""

from __future__ import annotations

from typing import Protocol


class ConversationOwnerBackend(Protocol):
    """跨连接/跨实例的单会话单连接发送守卫。"""

    async def claim_or_refresh(self, conversation_id: str, owner_token: str) -> bool:
        """
        尝试获取或续租会话写入所有权。

        返回:
        - True: 当前 owner_token 已获取/续租成功，可继续处理该会话消息
        - False: 会话当前已有其它连接在发送
        """

    async def release(self, conversation_id: str, owner_token: str) -> None:
        """仅当 owner_token 仍匹配时释放所有权。"""

    async def close(self) -> None:
        """释放底层连接资源。"""
