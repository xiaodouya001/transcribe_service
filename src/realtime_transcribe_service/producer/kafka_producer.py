"""Kafka producer — conversationId routing, acks=all, zstd compression, and fast failure.

- **Local docker-compose** (`KAFKA_MODE=admin`): ``PLAINTEXT`` only; the service may auto-create
  the topic via the Kafka Admin API. Not used when ``APP_ENV=deployed``.
- **AWS MSK with IAM** (`KAFKA_MODE=aws_msk`): MSK IAM via ``SASL_SSL`` + ``OAUTHBEARER`` and
  `aws-msk-iam-sasl-signer-python` (not SCRAM / SASL PLAIN). Required for deployed / remote Kafka.
"""

from __future__ import annotations

import asyncio
import ssl
import threading
import time
from typing import Any

import orjson
import structlog
from aiokafka import AIOKafkaProducer
from aiokafka.abc import AbstractTokenProvider
from aiokafka.admin import AIOKafkaAdminClient, NewTopic

log = structlog.get_logger(__name__)


def _generate_msk_auth_token(region: str, *, aws_debug_creds: bool = False) -> tuple[str, int]:
    """Generate an MSK IAM auth token using the default AWS credential chain."""
    try:
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider  # pyright: ignore[reportMissingImports]
    except ImportError as exc:  # pragma: no cover - exercised when dependency is missing
        raise RuntimeError(
            "aws-msk-iam-sasl-signer-python is required when KAFKA_MODE=aws_msk"
        ) from exc
    return MSKAuthTokenProvider.generate_auth_token(
        region,
        aws_debug_creds=aws_debug_creds,
    )


class MSKTokenProvider(AbstractTokenProvider):
    """Async aiokafka token provider backed by the AWS MSK IAM signer."""

    def __init__(self, region: str, *, aws_debug_creds: bool = False) -> None:
        self._region = region
        self._aws_debug_creds = aws_debug_creds
        self._token: str | None = None
        self._expiry_ms = 0
        self._lock = threading.Lock()

    def _refresh_token(self) -> str:
        now_ms = int(time.time() * 1000)
        with self._lock:
            if self._token is not None and now_ms < self._expiry_ms - 60_000:
                return self._token

            token, expiry_ms = _generate_msk_auth_token(
                self._region,
                aws_debug_creds=self._aws_debug_creds,
            )
            self._token = token
            self._expiry_ms = expiry_ms
            return token

    async def token(self) -> str:  # pyright: ignore[reportIncompatibleMethodOverride]  # upstream ABC omits return type
        return await asyncio.get_running_loop().run_in_executor(None, self._refresh_token)


def _build_kafka_client_kwargs(
    bootstrap_servers: str,
    *,
    mode: str = "admin",
    ssl_ca_file: str | None = None,
    aws_region: str | None = None,
    aws_debug_creds: bool = False,
) -> dict[str, Any]:
    """Build shared Kafka connection kwargs for admin and producer clients."""
    kwargs: dict[str, Any] = {"bootstrap_servers": bootstrap_servers}

    if mode == "aws_msk":
        if aws_region is None:
            raise ValueError("aws_region is required when mode=aws_msk")
        kwargs["security_protocol"] = "SASL_SSL"
        kwargs["ssl_context"] = ssl.create_default_context(cafile=ssl_ca_file)
        kwargs["sasl_mechanism"] = "OAUTHBEARER"
        kwargs["sasl_oauth_token_provider"] = MSKTokenProvider(
            aws_region,
            aws_debug_creds=aws_debug_creds,
        )
        return kwargs

    kwargs["security_protocol"] = "PLAINTEXT"
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
                error=repr(exc),
                exc_info=True,
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
        ssl_ca_file: str | None = None,
        aws_region: str | None = None,
        aws_debug_creds: bool = False,
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
            mode=mode,
            ssl_ca_file=ssl_ca_file,
            aws_region=aws_region,
            aws_debug_creds=aws_debug_creds,
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
