"""Redis sequence state machine — atomic Lua prepare/commit/cleanup."""

from __future__ import annotations

import structlog
from redis.asyncio import Redis
from redis.exceptions import NoScriptError

from realtime_transcribe_service.redis.protocols import PrepareOutcome, PrepareResult

log = structlog.get_logger(__name__)

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
        active_ttl_sec: int,
        final_ttl_sec: int,
        key_prefix: str,
        client: Redis | None = None,
    ) -> None:
        if client is None and redis_url is None:
            raise ValueError("redis_url is required when client is not provided")
        self._redis_url = redis_url
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
            self._client = Redis.from_url(
                self._redis_url,
                decode_responses=True,
                max_connections=self._max_connections,
            )
        return self._client

    async def _ensure_scripts_loaded(self) -> None:
        client = await self._get_client()
        if self._sha_prepare is None:
            self._sha_prepare = await client.script_load(LUA_PREPARE)
        if self._sha_commit is None:
            self._sha_commit = await client.script_load(LUA_COMMIT)
        if self._sha_cleanup is None:
            self._sha_cleanup = await client.script_load(LUA_CLEANUP)

    def _key(self, conversation_id: str) -> str:
        return f"{self._key_prefix}:{conversation_id}"

    @staticmethod
    def _parse_prepare_result(result: list[object] | tuple[object, ...]) -> PrepareOutcome:
        if len(result) != 2:
            raise ValueError(f"Unexpected prepare result shape: {result!r}")

        status_raw, expected_raw = result
        expected_sequence = None if expected_raw is None else int(expected_raw)
        return PrepareOutcome(
            status=PrepareResult(str(status_raw)),
            expected_sequence=expected_sequence,
        )

    async def prepare(self, conversation_id: str, seq: int) -> PrepareOutcome:
        client = await self._get_client()
        await self._ensure_scripts_loaded()
        try:
            raw_result: list[object] | tuple[object, ...] = await client.evalsha(
                self._sha_prepare, 1, self._key(conversation_id), seq, self._active_ttl
            )
        except NoScriptError:
            self._sha_prepare = await client.script_load(LUA_PREPARE)
            raw_result = await client.evalsha(
                self._sha_prepare, 1, self._key(conversation_id), seq, self._active_ttl
            )
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
        try:
            await client.evalsha(
                self._sha_commit, 1, self._key(conversation_id), seq, self._active_ttl
            )
        except NoScriptError:
            self._sha_commit = await client.script_load(LUA_COMMIT)
            await client.evalsha(
                self._sha_commit, 1, self._key(conversation_id), seq, self._active_ttl
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
        try:
            await client.evalsha(
                self._sha_cleanup, 1, self._key(conversation_id), self._final_ttl
            )
        except NoScriptError:
            self._sha_cleanup = await client.script_load(LUA_CLEANUP)
            await client.evalsha(
                self._sha_cleanup, 1, self._key(conversation_id), self._final_ttl
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

