"""Kafka producer for production - aiokafka with session_id as key."""

from aiokafka import AIOKafkaProducer

from asr_ingest.producer.base import ProducerBackend


class KafkaProducer:
    """Kafka producer: session_id as key, lz4, idempotence, linger_ms."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "asr_realtime_text",
    ) -> None:
        self._bootstrap = bootstrap_servers
        self._topic = topic
        self._producer: AIOKafkaProducer | None = None

    async def _get_producer(self) -> AIOKafkaProducer:
        if self._producer is None:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self._bootstrap,
                compression_type="lz4",
                enable_idempotence=True,
                linger_ms=15,
            )
            await self._producer.start()
        return self._producer

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
                **{k: v for k, v in kwargs.items() if k not in ("raw_payload", "cleaned", "source_json")},
            }
            payload = {"raw": None, "cleaned": cleaned_dict}
        value = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        key = session_id.encode("utf-8")
        producer = await self._get_producer()
        await producer.send_and_wait(self._topic, value=value, key=key)

    async def flush(self) -> None:
        """Flush producer buffers."""
        if self._producer:
            await self._producer.flush()

    async def close(self) -> None:
        """Stop producer."""
        if self._producer:
            await self._producer.stop()
            self._producer = None
