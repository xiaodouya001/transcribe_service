"""Redis-backed deduplication using SETNX."""

from redis.asyncio import Redis

DEDUP_KEY_PREFIX = "dedup"
DEDUP_TTL_SECONDS = 10


def _build_key(
    parts_config: str,
    session_id: str,
    seq_no: int,
    *,
    processing_id: str = "",
    created_at: str = "",
    **kwargs: str,
) -> str:
    """Build dedup key from config. Parts: session_id, processing_id, seq_no, created_at."""
    parts: list[str] = []
    for name in (p.strip() for p in parts_config.split(",") if p.strip()):
        if name == "session_id":
            parts.append(session_id)
        elif name == "processing_id":
            parts.append(processing_id or "")
        elif name == "seq_no":
            parts.append(str(seq_no))
        elif name == "created_at":
            parts.append(created_at or "")
        elif name in kwargs:
            parts.append(str(kwargs[name]))
    return f"{DEDUP_KEY_PREFIX}:{':'.join(parts)}"


class RedisDedup:
    """Deduplication via Redis SETNX. Key format configurable via dedup_key_parts, TTL 10s."""

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        dedup_key_parts: str = "session_id,processing_id,seq_no",
    ) -> None:
        self._redis_url = redis_url
        self._dedup_key_parts = dedup_key_parts
        self._client: Redis | None = None

    async def _get_client(self) -> Redis:
        if self._client is None:
            self._client = Redis.from_url(self._redis_url, decode_responses=True)
        return self._client

    def _key(
        self,
        session_id: str,
        seq_no: int,
        *,
        processing_id: str = "",
        created_at: str = "",
        **kwargs: str,
    ) -> str:
        return _build_key(
            self._dedup_key_parts,
            session_id,
            seq_no,
            processing_id=processing_id,
            created_at=created_at,
            **kwargs,
        )

    async def should_emit(
        self,
        session_id: str,
        seq_no: int,
        *,
        processing_id: str = "",
        created_at: str = "",
        **kwargs: str,
    ) -> bool:
        """SETNX: return True if key was set (first time), False if already exists."""
        client = await self._get_client()
        key = self._key(
            session_id,
            seq_no,
            processing_id=processing_id,
            created_at=created_at,
            **kwargs,
        )
        ok = await client.set(key, "1", nx=True, ex=DEDUP_TTL_SECONDS)
        return bool(ok)

    async def cleanup_session(self, session_id: str) -> None:
        """Scan and delete keys for session. Optional; TTL will expire them anyway."""
        client = await self._get_client()
        pattern = f"{DEDUP_KEY_PREFIX}:{session_id}:*"
        cursor = 0
        while True:
            cursor, keys = await client.scan(cursor, match=pattern, count=100)
            if keys:
                await client.delete(*keys)
            if cursor == 0:
                break

    async def close(self) -> None:
        """Close Redis connection."""
        if self._client:
            await self._client.aclose()
            self._client = None
