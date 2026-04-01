"""Construct redis.asyncio clients with shared TLS and credential kwargs."""

from __future__ import annotations

from typing import TypedDict
from urllib.parse import urlparse

from redis.asyncio import Redis


class _RedisClientKwargs(TypedDict, total=False):
    decode_responses: bool
    max_connections: int
    ssl_check_hostname: bool
    username: str
    password: str


def create_async_redis_client(
    redis_url: str,
    *,
    username: str | None = None,
    password: str | None = None,
    ssl_check_hostname: bool = False,
    decode_responses: bool = True,
    max_connections: int = 100,
) -> Redis:
    """Build a :class:`~redis.asyncio.Redis` client.

    Credentials are passed explicitly so ``REDIS_URL`` need not embed ``user:pass@``.
    ``ssl_check_hostname`` maps to redis-py's TLS option (relevant for ``rediss://``).
    """
    kwargs: _RedisClientKwargs = {
        "decode_responses": decode_responses,
        "max_connections": max_connections,
    }
    # Plain TCP connections reject ssl_check_hostname; only pass for TLS (rediss).
    if urlparse(redis_url).scheme.lower() == "rediss":
        kwargs["ssl_check_hostname"] = ssl_check_hostname
    u = username.strip() if username else None
    if u:
        kwargs["username"] = u
    if password is not None and password != "":
        kwargs["password"] = password
    return Redis.from_url(redis_url, **kwargs)
