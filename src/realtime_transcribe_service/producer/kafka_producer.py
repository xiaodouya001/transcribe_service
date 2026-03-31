"""Kafka producer — conversationId routing, acks=all, zstd compression, and fast failure."""

from __future__ import annotations

import asyncio
import ssl
from typing import Any

import orjson
import structlog
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

log = structlog.get_logger(__name__)


def _build_kafka_client_kwargs(
    bootstrap_servers: str,
    *,
    security_protocol: str = "PLAINTEXT",
    sasl_mechanism: str | None = None,
    sasl_username: str | None = None,
    sasl_password: str | None = None,
) -> dict[str, Any]:
    """Build shared Kafka connection kwargs for admin and producer clients."""
    kwargs: dict[str, Any] = {
        "bootstrap_servers": bootstrap_servers,
        "security_protocol": security_protocol,
    }
    if security_protocol in {"SSL", "SASL_SSL"}:
        kwargs["ssl_context"] = ssl.create_default_context()
    if security_protocol.startswith("SASL_"):
        kwargs["sasl_mechanism"] = sasl_mechanism
        kwargs["sasl_plain_username"] = sasl_username
        kwargs["sasl_plain_password"] = sasl_password
    return kwargs


async def _ensure_topic(
    bootstrap_servers: str,
    topic: str,
    num_partitions: int,
    replication_factor: int = 1,
    *,
    client_kwargs: dict[str, Any] | None = None,
) -> None:
    """Create the topic idempotently and ignore the "already exists" case."""
    admin = AIOKafkaAdminClient(**(client_kwargs or {"bootstrap_servers": bootstrap_servers}))
    await admin.start()
    try:
        await admin.create_topics(
            [NewTopic(name=topic, num_partitions=num_partitions, replication_factor=replication_factor)]
        )
    except Exception as exc:
        err_text = str(exc).lower()
        if any(token in err_text for token in ("exist", "already exists", "topic already")):
            log.debug(
                "Kafka: Topic already exists, continuing startup",
                bootstrap_servers=bootstrap_servers,
                topic=topic,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                exc_type=type(exc).__name__,
                error=str(exc),
            )
        else:
            log.warning(
                "Kafka: Topic creation failed during startup",
                bootstrap_servers=bootstrap_servers,
                topic=topic,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                exc_type=type(exc).__name__,
                error=str(exc),
            )
            raise
    finally:
        await admin.close()


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
        topic: str = "AI_STAGING_TRANSCRIPTION",
        *,
        mode: str = "admin",
        compression_type: str = "zstd",
        security_protocol: str = "PLAINTEXT",
        sasl_mechanism: str | None = None,
        sasl_username: str | None = None,
        sasl_password: str | None = None,
        send_timeout_sec: float = 2.0,
        linger_ms: int = 1,
        batch_size: int = 32768,
        num_partitions: int = 50,
        replication_factor: int = 1,
    ) -> None:
        self._bootstrap = bootstrap_servers
        self._topic = topic
        self._mode = mode
        self._compression_type = compression_type
        self._client_kwargs = _build_kafka_client_kwargs(
            bootstrap_servers,
            security_protocol=security_protocol,
            sasl_mechanism=sasl_mechanism,
            sasl_username=sasl_username,
            sasl_password=sasl_password,
        )
        self._send_timeout_sec = send_timeout_sec
        self._linger_ms = linger_ms
        self._batch_size = batch_size
        self._num_partitions = num_partitions
        self._replication_factor = replication_factor
        self._producer: AIOKafkaProducer | None = None

    async def _get_producer(self) -> AIOKafkaProducer:
        if self._producer is None:
            if self._mode == "admin":
                await _ensure_topic(
                    self._bootstrap,
                    self._topic,
                    self._num_partitions,
                    self._replication_factor,
                    client_kwargs=self._client_kwargs,
                )
            else:
                log.info(
                    "Kafka: Topic auto-creation disabled",
                    bootstrap_servers=self._bootstrap,
                    topic=self._topic,
                    mode=self._mode,
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
            log.error(
                "Kafka: Send failed",
                conversation_id=conversation_id,
                topic=self._topic,
                error=str(e),
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
