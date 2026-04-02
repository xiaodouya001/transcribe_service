"""Redis sequence state machine — atomic Lua prepare/commit/cleanup."""

from __future__ import annotations

from collections.abc import Awaitable
from typing import cast

from redis.asyncio import Redis
from redis.exceptions import NoScriptError

from realtime_transcribe_service.config.logging_config import get_logger
from realtime_transcribe_service.redis.async_client import create_async_redis_client
from realtime_transcribe_service.redis.protocols import PrepareOutcome, PrepareResult

log = get_logger(__name__)
RedisEvalArg = str | int | float | bytes | bytearray | memoryview

LUA_PREPARE = """
local key = KEYS[1]
local incoming = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local current = redis.call('GET', key)

if current == false then
    if incoming == 0 then
        redis.call('SET', key, 0, 'EX', ttl)
        return {'PRE_CHECK_OK', 0}
    else
        return {'OUT_OF_ORDER', 0}
    end
end

current = tonumber(current)

if incoming == current then
    redis.call('EXPIRE', key, ttl)
    return {'PRE_CHECK_OK', current}
elseif incoming < current then
    return {'IDEMPOTENT', current}
else
    return {'OUT_OF_ORDER', current}
end
"""

LUA_COMMIT = """
local key = KEYS[1]
local seq = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
redis.call('SET', key, seq + 1, 'EX', ttl)
return 'OK'
"""

LUA_CLEANUP = """
local key = KEYS[1]
local final_ttl = tonumber(ARGV[1])
if redis.call('EXISTS', key) == 1 then
    redis.call('EXPIRE', key, final_ttl)
end
return 'OK'
"""


class RedisSequenceStateMachine:
    """Redis Lua-backed optimistic sequence state machine."""

    def __init__(
        self,
        redis_url: str | None = None,
        *,
        max_connections: int = 100,
        redis_username: str | None = None,
        redis_password: str | None = None,
        ssl_check_hostname: bool = False,
        active_ttl_sec: int,
        final_ttl_sec: int,
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
        self._active_ttl = active_ttl_sec
        self._final_ttl = final_ttl_sec
        self._key_prefix = key_prefix
        self._client: Redis | None = client
        self._client_injected = client is not None
        self._sha_prepare: str | None = None
        self._sha_commit: str | None = None
        self._sha_cleanup: str | None = None

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
        if self._sha_prepare is None:
            self._sha_prepare = await self._script_load(client, LUA_PREPARE)
        if self._sha_commit is None:
            self._sha_commit = await self._script_load(client, LUA_COMMIT)
        if self._sha_cleanup is None:
            self._sha_cleanup = await self._script_load(client, LUA_CLEANUP)

    def _key(self, conversation_id: str) -> str:
        return f"{self._key_prefix}:{conversation_id}"

    @staticmethod
    def _parse_prepare_result(result: list[object] | tuple[object, ...]) -> PrepareOutcome:
        if len(result) != 2:
            raise ValueError(f"Unexpected prepare result shape: {result!r}")

        status_raw, expected_raw = result
        if expected_raw is None:
            expected_sequence = None
        elif isinstance(expected_raw, int):
            expected_sequence = expected_raw
        elif isinstance(expected_raw, str):
            expected_sequence = int(expected_raw)
        else:
            raise ValueError(f"Unexpected expected_sequence type: {type(expected_raw).__name__}")

        if isinstance(status_raw, bytes):
            status_value = status_raw.decode("utf-8")
        else:
            status_value = str(status_raw)
        return PrepareOutcome(
            status=PrepareResult(status_value),
            expected_sequence=expected_sequence,
        )

    async def prepare(self, conversation_id: str, seq: int) -> PrepareOutcome:
        client = await self._get_client()
        await self._ensure_scripts_loaded()
        assert self._sha_prepare is not None
        try:
            raw_result = await self._evalsha(
                client, self._sha_prepare, 1, self._key(conversation_id), seq, self._active_ttl
            )
        except NoScriptError:
            self._sha_prepare = await self._script_load(client, LUA_PREPARE)
            raw_result = await self._evalsha(
                client, self._sha_prepare, 1, self._key(conversation_id), seq, self._active_ttl
            )
        if not isinstance(raw_result, (list, tuple)):
            raise ValueError(f"Unexpected prepare result type: {type(raw_result).__name__}")
        outcome = self._parse_prepare_result(raw_result)
        log.debug(
            "StateMachine.prepare",
            conversation_id=conversation_id,
            seq=seq,
            result=outcome.status.value,
            expected_sequence=outcome.expected_sequence,
        )
        return outcome

    async def commit(self, conversation_id: str, seq: int) -> None:
        client = await self._get_client()
        await self._ensure_scripts_loaded()
        assert self._sha_commit is not None
        try:
            await self._evalsha(
                client, self._sha_commit, 1, self._key(conversation_id), seq, self._active_ttl
            )
        except NoScriptError:
            self._sha_commit = await self._script_load(client, LUA_COMMIT)
            await self._evalsha(
                client, self._sha_commit, 1, self._key(conversation_id), seq, self._active_ttl
            )
        log.debug(
            "StateMachine.commit",
            conversation_id=conversation_id,
            seq=seq,
            next_expected=seq + 1,
        )

    async def cleanup(self, conversation_id: str) -> None:
        client = await self._get_client()
        await self._ensure_scripts_loaded()
        assert self._sha_cleanup is not None
        try:
            await self._evalsha(
                client, self._sha_cleanup, 1, self._key(conversation_id), self._final_ttl
            )
        except NoScriptError:
            self._sha_cleanup = await self._script_load(client, LUA_CLEANUP)
            await self._evalsha(
                client, self._sha_cleanup, 1, self._key(conversation_id), self._final_ttl
            )
        log.info(
            "StateMachine.cleanup",
            conversation_id=conversation_id,
            final_ttl=self._final_ttl,
        )

    async def close(self) -> None:
        if self._client is not None and not self._client_injected:
            await self._client.aclose()
        self._client = None

