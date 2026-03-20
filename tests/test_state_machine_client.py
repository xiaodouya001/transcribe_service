"""coverage: RedisStateMachine._get_client / close without injection."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from transcribe_service.state_machine.base import PrepareResult
from transcribe_service.state_machine.redis_state import RedisStateMachine


@pytest.mark.asyncio
async def test_get_client_lazy_and_close_calls_aclose():
    fake_redis = MagicMock()
    fake_redis.eval = AsyncMock(return_value="PRE_CHECK_OK")
    fake_redis.aclose = AsyncMock()

    with patch("transcribe_service.state_machine.redis_state.Redis") as R:
        R.from_url.return_value = fake_redis
        sm = RedisStateMachine(redis_url="redis://localhost:6379/0", max_connections=5)
        assert sm._client is None
        r = await sm.prepare("c1", 0)
        assert r == PrepareResult.PRE_CHECK_OK
        R.from_url.assert_called_once()
        await sm.close()
        fake_redis.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_close_skips_aclose_for_injected_client():
    injected = MagicMock()
    injected.eval = AsyncMock(return_value="OK")
    injected.aclose = AsyncMock()
    sm = RedisStateMachine(client=injected)
    await sm.close()
    injected.aclose.assert_not_awaited()
