"""Tests for state_machine 状态机层（使用 fakeredis）。"""

import pytest
import fakeredis.aioredis
from redis.exceptions import NoScriptError
from unittest.mock import AsyncMock, MagicMock

from transcribe_service.redis.protocols import PrepareResult
from transcribe_service.redis.sequence_state_machine import RedisSequenceStateMachine


@pytest.fixture
async def sm():
    """RedisSequenceStateMachine with fakeredis backend."""
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    machine = RedisSequenceStateMachine(
        client=client,
        active_ttl_sec=3600,
        final_ttl_sec=60,
        key_prefix="transcript:session",
    )
    yield machine
    await machine.close()


class TestPrepare:
    async def test_first_message_seq_0_ok(self, sm: RedisSequenceStateMachine):
        result = await sm.prepare("conv-1", 0)
        assert result == PrepareResult.PRE_CHECK_OK

    async def test_first_message_seq_nonzero_out_of_order(self, sm: RedisSequenceStateMachine):
        result = await sm.prepare("conv-1", 5)
        assert result == PrepareResult.OUT_OF_ORDER

    async def test_sequential_messages(self, sm: RedisSequenceStateMachine):
        r0 = await sm.prepare("conv-1", 0)
        assert r0 == PrepareResult.PRE_CHECK_OK
        await sm.commit("conv-1", 0)

        r1 = await sm.prepare("conv-1", 1)
        assert r1 == PrepareResult.PRE_CHECK_OK

    async def test_duplicate_message_idempotent(self, sm: RedisSequenceStateMachine):
        await sm.prepare("conv-1", 0)
        await sm.commit("conv-1", 0)

        result = await sm.prepare("conv-1", 0)
        assert result == PrepareResult.IDEMPOTENT

    async def test_out_of_order_skip(self, sm: RedisSequenceStateMachine):
        await sm.prepare("conv-1", 0)
        await sm.commit("conv-1", 0)

        result = await sm.prepare("conv-1", 5)
        assert result == PrepareResult.OUT_OF_ORDER

    async def test_no_incr_on_prepare(self, sm: RedisSequenceStateMachine):
        """PRE_CHECK_OK 后不 INCR，重复 prepare 同一 seq 应仍为 PRE_CHECK_OK。"""
        r1 = await sm.prepare("conv-1", 0)
        assert r1 == PrepareResult.PRE_CHECK_OK

        r2 = await sm.prepare("conv-1", 0)
        assert r2 == PrepareResult.PRE_CHECK_OK


class TestCommit:
    async def test_commit_advances_expected(self, sm: RedisSequenceStateMachine):
        await sm.prepare("conv-1", 0)
        await sm.commit("conv-1", 0)

        r = await sm.prepare("conv-1", 1)
        assert r == PrepareResult.PRE_CHECK_OK

    async def test_commit_then_old_seq_idempotent(self, sm: RedisSequenceStateMachine):
        await sm.prepare("conv-1", 0)
        await sm.commit("conv-1", 0)

        await sm.prepare("conv-1", 1)
        await sm.commit("conv-1", 1)

        r = await sm.prepare("conv-1", 0)
        assert r == PrepareResult.IDEMPOTENT

    async def test_commit_reload_script_after_noscript(self):
        client = MagicMock()
        client.evalsha = AsyncMock(side_effect=[NoScriptError(), None])
        client.script_load = AsyncMock(return_value="sha-commit-new")

        sm = RedisSequenceStateMachine(
            client=client,
            active_ttl_sec=3600,
            final_ttl_sec=60,
            key_prefix="transcript:session",
        )
        sm._sha_commit = "sha-commit-old"
        sm._ensure_scripts_loaded = AsyncMock()

        await sm.commit("conv-1", 7)

        assert sm._sha_commit == "sha-commit-new"
        client.script_load.assert_awaited_once()
        assert client.evalsha.await_count == 2


class TestCleanup:
    async def test_cleanup_sets_short_ttl(self, sm: RedisSequenceStateMachine):
        await sm.prepare("conv-1", 0)
        await sm.commit("conv-1", 0)
        await sm.cleanup("conv-1")

        client = await sm._get_client()
        ttl = await client.ttl("transcript:session:conv-1")
        assert 0 < ttl <= 60

    async def test_cleanup_reload_script_after_noscript(self):
        client = MagicMock()
        client.evalsha = AsyncMock(side_effect=[NoScriptError(), None])
        client.script_load = AsyncMock(return_value="sha-cleanup-new")

        sm = RedisSequenceStateMachine(
            client=client,
            active_ttl_sec=3600,
            final_ttl_sec=60,
            key_prefix="transcript:session",
        )
        sm._sha_cleanup = "sha-cleanup-old"
        sm._ensure_scripts_loaded = AsyncMock()

        await sm.cleanup("conv-1")

        assert sm._sha_cleanup == "sha-cleanup-new"
        client.script_load.assert_awaited_once()
        assert client.evalsha.await_count == 2


class TestIsolation:
    async def test_different_conversations_isolated(self, sm: RedisSequenceStateMachine):
        await sm.prepare("conv-A", 0)
        await sm.commit("conv-A", 0)

        r = await sm.prepare("conv-B", 0)
        assert r == PrepareResult.PRE_CHECK_OK

        r = await sm.prepare("conv-A", 1)
        assert r == PrepareResult.PRE_CHECK_OK
