"""Redis-backed conversation sender-connection owner."""

from __future__ import annotations

import structlog
from redis.asyncio import Redis
from redis.exceptions import NoScriptError

log = structlog.get_logger(__name__)

KEY_PREFIX = "transcript:owner"
OWNER_TTL_SEC = 30

LUA_CLAIM_OR_REFRESH = """
local key = KEYS[1]
local owner = ARGV[1]
local ttl = tonumber(ARGV[2])
local current = redis.call('GET', key)

if current == false or current == owner then
    redis.call('SET', key, owner, 'EX', ttl)
    return 'OWNED'
end

return 'BUSY'
"""

LUA_RELEASE_IF_OWNER = """
local key = KEYS[1]
local owner = ARGV[1]
local current = redis.call('GET', key)

if current == owner then
    redis.call('DEL', key)
    return 1
end

return 0
"""


class RedisConversationOwner:
    """用 Redis key 为单个 conversationId 提供“单连接发送”约束。"""

    def __init__(
        self,
        redis_url: str = "redis://127.0.0.1:6379/0",
        *,
        max_connections: int = 100,
        owner_ttl_sec: int = OWNER_TTL_SEC,
        client: Redis | None = None,
    ) -> None:
        self._redis_url = redis_url
        self._max_connections = max_connections
        self._owner_ttl_sec = owner_ttl_sec
        self._client: Redis | None = client
        self._client_injected = client is not None
        self._sha_claim: str | None = None
        self._sha_release: str | None = None

    async def _get_client(self) -> Redis:
        if self._client is None:
            self._client = Redis.from_url(
                self._redis_url,
                decode_responses=True,
                max_connections=self._max_connections,
            )
        return self._client

    async def _ensure_scripts_loaded(self) -> None:
        client = await self._get_client()
        if self._sha_claim is None:
            self._sha_claim = await client.script_load(LUA_CLAIM_OR_REFRESH)
        if self._sha_release is None:
            self._sha_release = await client.script_load(LUA_RELEASE_IF_OWNER)

    @staticmethod
    def _key(conversation_id: str) -> str:
        return f"{KEY_PREFIX}:{conversation_id}"

    async def claim_or_refresh(self, conversation_id: str, owner_token: str) -> bool:
        client = await self._get_client()
        await self._ensure_scripts_loaded()
        try:
            result: str = await client.evalsha(
                self._sha_claim, 1, self._key(conversation_id), owner_token, self._owner_ttl_sec
            )
        except NoScriptError:
            self._sha_claim = await client.script_load(LUA_CLAIM_OR_REFRESH)
            result = await client.evalsha(
                self._sha_claim, 1, self._key(conversation_id), owner_token, self._owner_ttl_sec
            )
        owned = result == "OWNED"
        log.debug(
            "ConversationOwner.claim_or_refresh",
            conversation_id=conversation_id,
            owner_token=owner_token,
            owned=owned,
        )
        return owned

    async def release(self, conversation_id: str, owner_token: str) -> None:
        client = await self._get_client()
        await self._ensure_scripts_loaded()
        try:
            released = await client.evalsha(
                self._sha_release, 1, self._key(conversation_id), owner_token
            )
        except NoScriptError:
            self._sha_release = await client.script_load(LUA_RELEASE_IF_OWNER)
            released = await client.evalsha(
                self._sha_release, 1, self._key(conversation_id), owner_token
            )
        log.debug(
            "ConversationOwner.release",
            conversation_id=conversation_id,
            owner_token=owner_token,
            released=bool(released),
        )

    async def close(self) -> None:
        if self._client is not None and not self._client_injected:
            await self._client.aclose()
        self._client = None
