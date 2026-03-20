"""Kafka 生产者 — conversationId 路由、acks=all、zstd 压缩、2s 快速失败。"""

from __future__ import annotations

import asyncio

import orjson
import structlog
from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from typing import Any

log = structlog.get_logger(__name__)


async def _ensure_topic(
    bootstrap_servers: str,
    topic: str,
    num_partitions: int,
    replication_factor: int = 1,
) -> None:
    """幂等建 Topic（已存在则忽略）。"""
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        await admin.create_topics(
            [NewTopic(name=topic, num_partitions=num_partitions, replication_factor=replication_factor)]
        )
    except Exception:
        pass
    finally:
        await admin.close()


class KafkaProducer:
    """Kafka 投递层实现。

    - Partition Key: conversationId
    - acks=all, enable_idempotence=True, max_in_flight_requests_per_connection=1
    - compression: zstd（可配置）
    - send timeout: 2s（可配置）
    """

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "cc.transcript.realtime.v1",
        *,
        compression_type: str = "zstd",
        send_timeout_sec: float = 2.0,
        linger_ms: int = 1,
        batch_size: int = 32768,
        num_partitions: int = 50,
        replication_factor: int = 1,
    ) -> None:
        self._bootstrap = bootstrap_servers
        self._topic = topic
        self._compression_type = compression_type
        self._send_timeout_sec = send_timeout_sec
        self._linger_ms = linger_ms
        self._batch_size = batch_size
        self._num_partitions = num_partitions
        self._replication_factor = replication_factor
        self._producer: AIOKafkaProducer | None = None

    async def _get_producer(self) -> AIOKafkaProducer:
        if self._producer is None:
            await _ensure_topic(
                self._bootstrap,
                self._topic,
                self._num_partitions,
                self._replication_factor,
            )
            comp = None if self._compression_type == "none" else self._compression_type
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self._bootstrap,
                compression_type=comp,
                enable_idempotence=True,
                max_request_size=1048576,
                linger_ms=self._linger_ms,
                max_batch_size=self._batch_size,
            )
            await self._producer.start()
        return self._producer

    async def ensure_ready(self) -> None:
        """启动时验证 Kafka 可达。"""
        await self._get_producer()

    async def send(
        self,
        conversation_id: str,
        payload: dict[str, Any],
    ) -> None:
        """发送消息到 Kafka，conversationId 作为 Key。"""
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
                "Kafka: 发送超时",
                conversation_id=conversation_id,
                topic=self._topic,
                timeout_sec=self._send_timeout_sec,
            )
            raise
        except Exception as e:
            log.error(
                "Kafka: 发送失败",
                conversation_id=conversation_id,
                topic=self._topic,
                error=str(e),
            )
            raise
        log.debug(
            "Kafka: 已发送",
            conversation_id=conversation_id,
            topic=self._topic,
        )

    async def flush(self) -> None:
        """刷新缓冲区。"""
        if self._producer:
            await self._producer.flush()

    async def close(self) -> None:
        """关闭生产者。"""
        if self._producer:
            await self._producer.stop()
            self._producer = None
