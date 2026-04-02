"""Runtime wiring helpers for Redis-backed dependencies."""

from __future__ import annotations

from collections.abc import Awaitable
from typing import Any, cast

from realtime_transcribe_service.config.settings import Settings
from realtime_transcribe_service.redis.async_client import create_async_redis_client
from realtime_transcribe_service.redis.ownership_guard import RedisConversationOwnershipGuard
from realtime_transcribe_service.redis.sequence_state_machine import RedisSequenceStateMachine


def create_shared_redis_client(settings: Settings) -> Any:
    """Build the shared Redis client used across startup checks, runtime, and readiness."""
    redis_url = settings.redis_url
    assert redis_url is not None
    return create_async_redis_client(
        redis_url,
        username=settings.redis_username,
        password=settings.redis_password,
        ssl_check_hostname=settings.redis_ssl_check_hostname,
        decode_responses=True,
        max_connections=settings.redis_max_connections,
    )


def create_sequence_state_machine(
    settings: Settings, *, client: Any
) -> RedisSequenceStateMachine:
    """Create the Redis sequence state machine using the shared Redis client."""
    redis_url = settings.redis_url
    assert redis_url is not None
    return RedisSequenceStateMachine(
        redis_url=redis_url,
        max_connections=settings.redis_max_connections,
        redis_username=settings.redis_username,
        redis_password=settings.redis_password,
        ssl_check_hostname=settings.redis_ssl_check_hostname,
        active_ttl_sec=settings.redis_active_ttl_sec,
        final_ttl_sec=settings.redis_final_ttl_sec,
        key_prefix=settings.redis_sequence_state_key_prefix,
        client=client,
    )


def create_ownership_guard(
    settings: Settings, *, client: Any
) -> RedisConversationOwnershipGuard:
    """Create the Redis ownership guard using the shared Redis client."""
    redis_url = settings.redis_url
    assert redis_url is not None
    return RedisConversationOwnershipGuard(
        redis_url=redis_url,
        max_connections=settings.redis_max_connections,
        redis_username=settings.redis_username,
        redis_password=settings.redis_password,
        ssl_check_hostname=settings.redis_ssl_check_hostname,
        guard_ttl_sec=settings.redis_ownership_guard_ttl_sec,
        key_prefix=settings.redis_ownership_guard_key_prefix,
        client=client,
    )


async def close_redis_runtime(
    *,
    client: Any | None,
    sequence_state_machine: RedisSequenceStateMachine | None,
    ownership_guard: RedisConversationOwnershipGuard | None,
) -> None:
    """Close Redis-backed runtime resources in deterministic order."""
    if sequence_state_machine is not None:
        await sequence_state_machine.close()
    if ownership_guard is not None:
        await ownership_guard.close()
    if client is not None:
        await cast(Awaitable[Any], client.aclose())
