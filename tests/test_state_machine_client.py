"""coverage: RedisSequenceStateMachine._get_client / close without injection."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from redis.exceptions import NoScriptError

from realtime_transcribe_service.redis.protocols import PrepareOutcome, PrepareResult
from realtime_transcribe_service.redis.sequence_state_machine import RedisSequenceStateMachine


@pytest.mark.asyncio
async def test_get_client_lazy_and_close_calls_aclose():
    fake_redis = MagicMock()
    fake_redis.script_load = AsyncMock(side_effect=["sha_prepare", "sha_commit", "sha_cleanup"])
    fake_redis.evalsha = AsyncMock(return_value=["PRE_CHECK_OK", 0])
    fake_redis.aclose = AsyncMock()

    with patch("realtime_transcribe_service.redis.sequence_state_machine.Redis") as R:
        R.from_url.return_value = fake_redis
        sm = RedisSequenceStateMachine(
            redis_url="redis://127.0.0.1:6379/0",
            max_connections=5,
            active_ttl_sec=3600,
            final_ttl_sec=60,
            key_prefix="realtime-transcribe-service:expect-transcript-seq-num",
        )
        assert sm._client is None
        r = await sm.prepare("c1", 0)
        assert r == PrepareOutcome(PrepareResult.PRE_CHECK_OK, expected_sequence=0)
        R.from_url.assert_called_once()
        await sm.close()
        fake_redis.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_close_skips_aclose_for_injected_client():
    injected = MagicMock()
    injected.script_load = AsyncMock(return_value="sha")
    injected.evalsha = AsyncMock(return_value="OK")
    injected.aclose = AsyncMock()
    sm = RedisSequenceStateMachine(
        client=injected,
        active_ttl_sec=3600,
        final_ttl_sec=60,
        key_prefix="realtime-transcribe-service:expect-transcript-seq-num",
    )
    await sm.close()
    injected.aclose.assert_not_awaited()


@pytest.mark.asyncio
async def test_prepare_reload_script_on_noscript():
    fake_redis = MagicMock()
    fake_redis.script_load = AsyncMock(
        side_effect=["sha_prepare", "sha_commit", "sha_cleanup", "sha_prepare_reloaded"]
    )
    fake_redis.evalsha = AsyncMock(side_effect=[NoScriptError("NOSCRIPT"), ["PRE_CHECK_OK", 0]])
    fake_redis.aclose = AsyncMock()

    with patch("realtime_transcribe_service.redis.sequence_state_machine.Redis") as R:
        R.from_url.return_value = fake_redis
        sm = RedisSequenceStateMachine(
            redis_url="redis://127.0.0.1:6379/0",
            active_ttl_sec=3600,
            final_ttl_sec=60,
            key_prefix="realtime-transcribe-service:expect-transcript-seq-num",
        )
        r = await sm.prepare("c1", 0)
        assert r == PrepareOutcome(PrepareResult.PRE_CHECK_OK, expected_sequence=0)
        assert fake_redis.script_load.await_count == 4
        assert fake_redis.evalsha.await_count == 2


@pytest.mark.asyncio
async def test_prepare_parses_expected_sequence_from_lua_tuple():
    fake_redis = MagicMock()
    fake_redis.script_load = AsyncMock(side_effect=["sha_prepare", "sha_commit", "sha_cleanup"])
    fake_redis.evalsha = AsyncMock(return_value=["OUT_OF_ORDER", 3])
    fake_redis.aclose = AsyncMock()

    with patch("realtime_transcribe_service.redis.sequence_state_machine.Redis") as R:
        R.from_url.return_value = fake_redis
        sm = RedisSequenceStateMachine(
            redis_url="redis://127.0.0.1:6379/0",
            active_ttl_sec=3600,
            final_ttl_sec=60,
            key_prefix="realtime-transcribe-service:expect-transcript-seq-num",
        )
        r = await sm.prepare("c1", 5)
        assert r == PrepareOutcome(PrepareResult.OUT_OF_ORDER, expected_sequence=3)


@pytest.mark.asyncio
async def test_prepare_raises_on_unexpected_lua_result_shape():
    fake_redis = MagicMock()
    fake_redis.script_load = AsyncMock(side_effect=["sha_prepare", "sha_commit", "sha_cleanup"])
    fake_redis.evalsha = AsyncMock(return_value=["OUT_OF_ORDER"])
    fake_redis.aclose = AsyncMock()

    with patch("realtime_transcribe_service.redis.sequence_state_machine.Redis") as R:
        R.from_url.return_value = fake_redis
        sm = RedisSequenceStateMachine(
            redis_url="redis://127.0.0.1:6379/0",
            active_ttl_sec=3600,
            final_ttl_sec=60,
            key_prefix="realtime-transcribe-service:expect-transcript-seq-num",
        )
        with pytest.raises(ValueError, match="Unexpected prepare result shape"):
            await sm.prepare("c1", 5)

