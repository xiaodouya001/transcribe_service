"""RedisBuffer - push raw payloads to Redis Stream for persistence."""

import json
import structlog
from redis.asyncio import Redis

log = structlog.get_logger(__name__)


class RedisBuffer:
    """Push raw vendor payloads to Redis Stream. XADD transcription:ingest:buffer."""

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        stream: str = "transcription:ingest:buffer",
        maxlen: int | None = 10000,
    ) -> None:
        self._redis_url = redis_url
        self._stream = stream
        self._maxlen = maxlen
        self._client: Redis | None = None

    async def _get_client(self) -> Redis:
        if self._client is None:
            self._client = Redis.from_url(self._redis_url, decode_responses=True)
        return self._client

    async def push(self, payload: dict) -> str:
        """Push raw payload to Stream. Returns message id."""
        client = await self._get_client()
        payload_str = json.dumps(payload, ensure_ascii=False)
        fields = {"payload": payload_str}
        if self._maxlen is not None:
            msg_id = await client.xadd(
                self._stream, fields, maxlen=self._maxlen, approximate=True
            )
        else:
            msg_id = await client.xadd(self._stream, fields)
        r = payload.get("result") or {}
        cs = r.get("callStatus") or {}
        log.info(
            "Buffer: 已写入 Redis Stream",
            msg_id=msg_id,
            session_id=cs.get("sessionId", ""),
            stream=self._stream,
        )
        return msg_id

    async def close(self) -> None:
        """Close Redis connection."""
        if self._client:
            await self._client.aclose()
            self._client = None
