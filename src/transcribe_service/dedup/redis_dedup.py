"""Redis-backed deduplication using SETNX."""

from redis.asyncio import Redis
import structlog

log = structlog.get_logger(__name__)
DEDUP_KEY_PREFIX = "dedup"


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


class RedisDeduplication:
    """Deduplication via Redis SETNX. Key format and TTL configurable via settings."""

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        dedup_key_parts: str = "session_id,processing_id,seq_no",
        dedup_ttl_seconds: int = 60,
        *,
        client: Redis | None = None,
        max_connections: int = 100,
    ) -> None:
        self._redis_url = redis_url
        self._dedup_key_parts = dedup_key_parts
        self._dedup_ttl_seconds = dedup_ttl_seconds
        self._max_connections = max_connections
        self._client: Redis | None = client
        self._client_injected = client is not None

    async def _get_client(self) -> Redis:
        if self._client is None:
            self._client = Redis.from_url(
                self._redis_url,
                decode_responses=True,
                max_connections=self._max_connections,
            )
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
        ok = await client.set(key, "1", nx=True, ex=self._dedup_ttl_seconds)
        if ok:
            log.debug(
                "Dedup: 通过（新 transcript）",
                session_id=session_id,
                seq_no=seq_no,
                processing_id=processing_id,
            )
        else:
            log.debug(
                "Dedup: 已过滤重复",
                session_id=session_id,
                seq_no=seq_no,
                processing_id=processing_id,
            )
        return bool(ok)

    async def remove(
        self,
        session_id: str,
        seq_no: int,
        *,
        processing_id: str = "",
        created_at: str = "",
        **kwargs: str,
    ) -> None:
        """Remove dedup key so event can be retried (e.g. after send failure)."""
        client = await self._get_client()
        key = self._key(
            session_id,
            seq_no,
            processing_id=processing_id,
            created_at=created_at,
            **kwargs,
        )
        await client.delete(key)

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
        """Close Redis connection. Skips if client was injected (e.g. fakeredis for tests)."""
        if self._client is not None and not self._client_injected:
            await self._client.aclose()
        self._client = None
