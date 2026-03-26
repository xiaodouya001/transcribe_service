"""coverage: redis.ownership_guard"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import fakeredis.aioredis
import pytest
from redis.exceptions import NoScriptError

from realtime_transcribe_service.redis.ownership_guard import RedisConversationOwnershipGuard


@pytest.mark.asyncio
async def test_claim_refresh_and_release_roundtrip():
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    owner = RedisConversationOwnershipGuard(
        client=client, guard_ttl_sec=30, key_prefix="real-time-transcriber:conversation-owner"
    )

    assert await owner.claim_or_refresh("conv-1", "owner-a") is True
    assert await owner.claim_or_refresh("conv-1", "owner-a") is True
    assert await owner.claim_or_refresh("conv-1", "owner-b") is False

    await owner.release("conv-1", "owner-b")
    assert await client.get("real-time-transcriber:conversation-owner:conv-1") == "owner-a"

    await owner.release("conv-1", "owner-a")
    assert await client.get("real-time-transcriber:conversation-owner:conv-1") is None


@pytest.mark.asyncio
async def test_claim_reacquires_after_ttl_expiry():
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    owner = RedisConversationOwnershipGuard(
        client=client, guard_ttl_sec=1, key_prefix="real-time-transcriber:conversation-owner"
    )

    assert await owner.claim_or_refresh("conv-1", "owner-a") is True
    await client.delete("real-time-transcriber:conversation-owner:conv-1")

    assert await owner.claim_or_refresh("conv-1", "owner-b") is True
    assert await client.get("real-time-transcriber:conversation-owner:conv-1") == "owner-b"


@pytest.mark.asyncio
async def test_get_client_lazy_and_close_calls_aclose():
    fake_redis = MagicMock()
    fake_redis.script_load = AsyncMock(side_effect=["sha_claim", "sha_release"])
    fake_redis.evalsha = AsyncMock(return_value="OWNED")
    fake_redis.aclose = AsyncMock()

    with patch("realtime_transcribe_service.redis.ownership_guard.Redis") as redis_cls:
        redis_cls.from_url.return_value = fake_redis
        owner = RedisConversationOwnershipGuard(
            redis_url="redis://127.0.0.1:6379/0",
            max_connections=5,
            guard_ttl_sec=30,
            key_prefix="real-time-transcriber:conversation-owner",
        )

        assert await owner.claim_or_refresh("conv-1", "owner-a") is True
        redis_cls.from_url.assert_called_once()

        await owner.close()
        fake_redis.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_close_skips_aclose_for_injected_client():
    injected = MagicMock()
    injected.script_load = AsyncMock(return_value="sha")
    injected.evalsha = AsyncMock(return_value="OWNED")
    injected.aclose = AsyncMock()

    owner = RedisConversationOwnershipGuard(
        client=injected, guard_ttl_sec=30, key_prefix="real-time-transcriber:conversation-owner"
    )
    await owner.close()

    injected.aclose.assert_not_awaited()


@pytest.mark.asyncio
async def test_claim_reload_script_after_noscript():
    fake_redis = MagicMock()
    fake_redis.script_load = AsyncMock(side_effect=["sha_claim", "sha_release", "sha_claim_reloaded"])
    fake_redis.evalsha = AsyncMock(side_effect=[NoScriptError("NOSCRIPT"), "OWNED"])
    fake_redis.aclose = AsyncMock()

    with patch("realtime_transcribe_service.redis.ownership_guard.Redis") as redis_cls:
        redis_cls.from_url.return_value = fake_redis
        owner = RedisConversationOwnershipGuard(
            redis_url="redis://127.0.0.1:6379/0",
            guard_ttl_sec=30,
            key_prefix="real-time-transcriber:conversation-owner",
        )

        assert await owner.claim_or_refresh("conv-1", "owner-a") is True
        assert fake_redis.script_load.await_count == 3
        assert fake_redis.evalsha.await_count == 2


@pytest.mark.asyncio
async def test_release_reload_script_after_noscript():
    fake_redis = MagicMock()
    fake_redis.script_load = AsyncMock(
        side_effect=["sha_claim", "sha_release", "sha_release_reloaded"]
    )
    fake_redis.evalsha = AsyncMock(side_effect=["OWNED", NoScriptError("NOSCRIPT"), 1])
    fake_redis.aclose = AsyncMock()

    with patch("realtime_transcribe_service.redis.ownership_guard.Redis") as redis_cls:
        redis_cls.from_url.return_value = fake_redis
        owner = RedisConversationOwnershipGuard(
            redis_url="redis://127.0.0.1:6379/0",
            guard_ttl_sec=30,
            key_prefix="real-time-transcriber:conversation-owner",
        )

        assert await owner.claim_or_refresh("conv-1", "owner-a") is True
        await owner.release("conv-1", "owner-a")
        assert fake_redis.script_load.await_count == 3
        assert fake_redis.evalsha.await_count == 3

