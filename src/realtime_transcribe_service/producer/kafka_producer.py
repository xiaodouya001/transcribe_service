"""Kafka producer — conversationId routing, acks=all, zstd compression, and fast failure.

Broker security is selected via :data:`~realtime_transcribe_service.producer.kafka_connection.KafkaBrokerConnection`
(:class:`~realtime_transcribe_service.producer.kafka_connection.LocalPlaintextKafkaConnection` or
:class:`~realtime_transcribe_service.producer.kafka_connection.AwsMskIamKafkaConnection`).
"""

from __future__ import annotations

import asyncio
from typing import Any

import orjson
from aiokafka import AIOKafkaProducer

from realtime_transcribe_service.config.logging_config import get_logger
from realtime_transcribe_service.constants import DEFAULT_KAFKA_TOPIC
from realtime_transcribe_service.producer.kafka_connection import (
    KafkaBrokerConnection,
    LocalPlaintextKafkaConnection,
)

log = get_logger(__name__)

_DEFAULT_LOCAL = LocalPlaintextKafkaConnection()


class KafkaProducer:
    """Kafka delivery-layer implementation.

    - Partition Key: conversationId
    - acks=all, enable_idempotence=True, max_in_flight_requests_per_connection=1
    - compression: zstd (configurable)
    - send timeout: 2s (configurable)
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str = DEFAULT_KAFKA_TOPIC,
        *,
        connection: KafkaBrokerConnection | None = None,
        compression_type: str = "zstd",
        send_timeout_sec: float = 2.0,
        linger_ms: int = 1,
        batch_size: int = 32768,
    ) -> None:
        self._bootstrap = bootstrap_servers
        self._topic = topic
        self._connection = connection if connection is not None else _DEFAULT_LOCAL
        self._compression_type = compression_type
        self._client_kwargs = self._connection.build_client_kwargs(
            bootstrap_servers=bootstrap_servers
        )
        self._send_timeout_sec = send_timeout_sec
        self._linger_ms = linger_ms
        self._batch_size = batch_size
        self._producer: AIOKafkaProducer | None = None

    async def _get_producer(self) -> AIOKafkaProducer:
        if self._producer is None:
            log.info(
                "Kafka: Topic must exist; auto-creation is disabled",
                bootstrap_servers=self._bootstrap,
                topic=self._topic,
                connection_profile=self._connection.profile_label,
            )
            comp = None if self._compression_type == "none" else self._compression_type
            self._producer = AIOKafkaProducer(
                **self._client_kwargs,
                compression_type=comp,
                enable_idempotence=True,
                max_request_size=1048576,
                linger_ms=self._linger_ms,
                max_batch_size=self._batch_size,
            )
            try:
                await self._producer.start()
            except Exception:
                await self.close()
                raise
        return self._producer

    async def ensure_ready(self) -> None:
        """Verify Kafka connectivity during startup."""
        try:
            await self._get_producer()
        except Exception:
            await self.close()
            raise

    async def send(
        self,
        conversation_id: str,
        payload: dict[str, Any],
    ) -> None:
        """Send a message to Kafka using ``conversationId`` as the key."""
        value = orjson.dumps(payload)
        key = conversation_id.encode("utf-8")
        producer = await self._get_producer()
        try:
            await asyncio.wait_for(
                producer.send_and_wait(self._topic, value=value, key=key),
                timeout=self._send_timeout_sec,
            )
        except asyncio.TimeoutError:
            log.error(
                "Kafka: Send timed out",
                conversation_id=conversation_id,
                topic=self._topic,
                timeout_sec=self._send_timeout_sec,
            )
            raise
        except Exception as e:
            log.exception(
                "Kafka: Send failed",
                conversation_id=conversation_id,
                topic=self._topic,
                error=repr(e),
                exc_type=type(e).__name__,
            )
            raise
        log.debug(
            "Kafka: Sent",
            conversation_id=conversation_id,
            topic=self._topic,
        )

    async def flush(self) -> None:
        """Flush producer buffers."""
        if self._producer:
            await self._producer.flush()

    async def close(self) -> None:
        """Close the producer."""
        if self._producer:
            await self._producer.stop()
            self._producer = None
