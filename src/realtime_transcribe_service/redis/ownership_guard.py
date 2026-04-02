"""Redis-backed conversation ownership guard."""

from __future__ import annotations

from collections.abc import Awaitable
from typing import cast

from redis.asyncio import Redis
from redis.exceptions import NoScriptError

from realtime_transcribe_service.config.logging_config import get_logger
from realtime_transcribe_service.redis.async_client import create_async_redis_client

log = get_logger(__name__)
RedisEvalArg = str | int | float | bytes | bytearray | memoryview

LUA_CLAIM_OR_REFRESH = """
local key = KEYS[1]
local token = ARGV[1]
local ttl = tonumber(ARGV[2])
local current = redis.call('GET', key)

if current == false or current == token then
    redis.call('SET', key, token, 'EX', ttl)
    return 'OWNED'
end

return 'BUSY'
"""

LUA_RELEASE_IF_OWNER = """
local key = KEYS[1]
local token = ARGV[1]
local current = redis.call('GET', key)

if current == token then
    redis.call('DEL', key)
    return 1
end

return 0
"""


class RedisConversationOwnershipGuard:
    """Redis-backed implementation of the conversation sender ownership guard."""

    def __init__(
        self,
        redis_url: str | None = None,
        *,
        max_connections: int = 100,
        redis_username: str | None = None,
        redis_password: str | None = None,
        ssl_check_hostname: bool = False,
        guard_ttl_sec: int,
        key_prefix: str,
        client: Redis | None = None,
    ) -> None:
        if client is None and redis_url is None:
            raise ValueError("redis_url is required when client is not provided")
        self._redis_url = redis_url
        self._redis_username = redis_username
        self._redis_password = redis_password
        self._ssl_check_hostname = ssl_check_hostname
        self._max_connections = max_connections
        self._guard_ttl_sec = guard_ttl_sec
        self._key_prefix = key_prefix
        self._client: Redis | None = client
        self._client_injected = client is not None
        self._sha_claim: str | None = None
        self._sha_release: str | None = None

    async def _get_client(self) -> Redis:
        if self._client is None:
            assert self._redis_url is not None
            self._client = create_async_redis_client(
                self._redis_url,
                username=self._redis_username,
                password=self._redis_password,
                ssl_check_hostname=self._ssl_check_hostname,
                decode_responses=True,
                max_connections=self._max_connections,
            )
        return self._client

    @staticmethod
    async def _script_load(client: Redis, script: str) -> str:
        return await cast(Awaitable[str], client.script_load(script))

    @staticmethod
    async def _evalsha(
        client: Redis, sha: str, numkeys: int, *keys_and_args: RedisEvalArg
    ) -> object:
        return await cast(Awaitable[object], client.evalsha(sha, numkeys, *keys_and_args))

    async def _ensure_scripts_loaded(self) -> None:
        client = await self._get_client()
        if self._sha_claim is None:
            self._sha_claim = await self._script_load(client, LUA_CLAIM_OR_REFRESH)
        if self._sha_release is None:
            self._sha_release = await self._script_load(client, LUA_RELEASE_IF_OWNER)

    def _key(self, conversation_id: str) -> str:
        return f"{self._key_prefix}:{conversation_id}"

    async def claim_or_refresh(self, conversation_id: str, ownership_token: str) -> bool:
        client = await self._get_client()
        await self._ensure_scripts_loaded()
        assert self._sha_claim is not None
        try:
            result = str(
                await self._evalsha(
                    client,
                    self._sha_claim,
                    1,
                    self._key(conversation_id),
                    ownership_token,
                    self._guard_ttl_sec,
                )
            )
        except NoScriptError:
            self._sha_claim = await self._script_load(client, LUA_CLAIM_OR_REFRESH)
            result = str(
                await self._evalsha(
                    client,
                    self._sha_claim,
                    1,
                    self._key(conversation_id),
                    ownership_token,
                    self._guard_ttl_sec,
                )
            )
        owned = result == "OWNED"
        log.debug(
            "ConversationOwnershipGuard.claim_or_refresh",
            conversation_id=conversation_id,
            ownership_token=ownership_token,
            owned=owned,
        )
        return owned

    async def release(self, conversation_id: str, ownership_token: str) -> None:
        client = await self._get_client()
        await self._ensure_scripts_loaded()
        assert self._sha_release is not None
        try:
            released = await self._evalsha(
                client, self._sha_release, 1, self._key(conversation_id), ownership_token
            )
        except NoScriptError:
            self._sha_release = await self._script_load(client, LUA_RELEASE_IF_OWNER)
            released = await self._evalsha(
                client, self._sha_release, 1, self._key(conversation_id), ownership_token
            )
        log.debug(
            "ConversationOwnershipGuard.release",
            conversation_id=conversation_id,
            ownership_token=ownership_token,
            released=bool(released),
        )

    async def close(self) -> None:
        if self._client is not None and not self._client_injected:
            await self._client.aclose()
        self._client = None
