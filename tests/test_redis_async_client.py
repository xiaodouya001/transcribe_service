"""coverage: redis.async_client.create_async_redis_client"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from realtime_transcribe_service.redis.async_client import create_async_redis_client


def test_create_async_redis_client_passes_kwargs_to_from_url():
    fake = MagicMock()
    with patch("redis.asyncio.Redis.from_url", return_value=fake) as from_url:
        out = create_async_redis_client(
            "rediss://cache.example:6379/0",
            username="u1",
            password="p1",
            ssl_check_hostname=False,
            decode_responses=True,
            max_connections=42,
        )
    assert out is fake
    from_url.assert_called_once_with(
        "rediss://cache.example:6379/0",
        decode_responses=True,
        max_connections=42,
        ssl_check_hostname=False,
        username="u1",
        password="p1",
    )


def test_create_async_redis_client_omits_empty_username_and_password():
    fake = MagicMock()
    with patch("redis.asyncio.Redis.from_url", return_value=fake) as from_url:
        create_async_redis_client(
            "redis://127.0.0.1:6379/0",
            username="   ",
            password="",
            ssl_check_hostname=True,
        )
    from_url.assert_called_once_with(
        "redis://127.0.0.1:6379/0",
        decode_responses=True,
        max_connections=100,
    )


def test_create_async_redis_client_passes_ssl_check_hostname_for_rediss():
    fake = MagicMock()
    with patch("redis.asyncio.Redis.from_url", return_value=fake) as from_url:
        create_async_redis_client(
            "rediss://cache:6379/0",
            ssl_check_hostname=False,
        )
    from_url.assert_called_once_with(
        "rediss://cache:6379/0",
        decode_responses=True,
        max_connections=100,
        ssl_check_hostname=False,
    )
