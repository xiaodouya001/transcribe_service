"""Kafka producer for production - aiokafka with session_id as key."""

import asyncio

import structlog
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

from transcription_ingest.producer.base import ProducerBackend

log = structlog.get_logger(__name__)


async def _ensure_topic(bootstrap_servers: str, topic: str) -> None:
    """Create topic if it does not exist."""
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        await admin.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
    except Exception:
        pass  # Topic may already exist
    finally:
        await admin.close()


class KafkaProducer:
    """Kafka producer: session_id as key, configurable compression, idempotence, linger_ms."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "transcription_topic",
        *,
        compression_type: str = "none",
        send_timeout_sec: float = 10.0,
    ) -> None:
        self._bootstrap = bootstrap_servers
        self._topic = topic
        self._compression_type = compression_type
        self._send_timeout_sec = send_timeout_sec
        self._producer: AIOKafkaProducer | None = None

    async def _get_producer(self) -> AIOKafkaProducer:
        if self._producer is None:
            await _ensure_topic(self._bootstrap, self._topic)
            comp = None if self._compression_type == "none" else self._compression_type
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self._bootstrap,
                compression_type=comp,
                enable_idempotence=True,
                linger_ms=15,
            )
            await self._producer.start()
        return self._producer

    async def ensure_ready(self) -> None:
        """Verify Kafka is reachable. Raises on connection failure."""
        await self._get_producer()

    async def send(
        self,
        session_id: str,
        seq_no: int,
        transcript: str,
        role: str = "",
        created_at: str = "",
        processing_status: str = "",
        *,
        raw_payload: dict | None = None,
        cleaned: dict | None = None,
        **kwargs: object,
    ) -> None:
        """Send to Kafka with session_id as key. Value: {raw, cleaned} when provided."""
        import json

        if raw_payload is not None or cleaned is not None:
            payload = {"raw": raw_payload, "cleaned": cleaned or {}}
        else:
            cleaned_dict = {
                "session_id": session_id,
                "seq_no": seq_no,
                "transcript": transcript,
                "role": role,
                "created_at": created_at,
                "processing_status": processing_status,
                **{k: v for k, v in kwargs.items() if k not in ("raw_payload", "cleaned")},
            }
            payload = {"raw": None, "cleaned": cleaned_dict}
        value = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        key = session_id.encode("utf-8")
        producer = await self._get_producer()
        await asyncio.wait_for(
            producer.send_and_wait(self._topic, value=value, key=key),
            timeout=self._send_timeout_sec,
        )
        log.info(
            "Kafka Producer: 已发送",
            session_id=session_id,
            seq_no=seq_no,
            topic=self._topic,
        )

    async def flush(self) -> None:
        """Flush producer buffers."""
        if self._producer:
            await self._producer.flush()

    async def close(self) -> None:
        """Stop producer."""
        if self._producer:
            await self._producer.stop()
            self._producer = None
