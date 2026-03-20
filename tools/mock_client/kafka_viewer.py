"""Kafka 消费者 — 订阅 topic 并广播消息给 SSE 订阅者。"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import Any

from aiokafka import AIOKafkaConsumer
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.admin.records_to_delete import RecordsToDelete
from aiokafka.errors import for_code
from aiokafka.structs import TopicPartition

log = logging.getLogger(__name__)


class KafkaViewer:
    """持续消费 Kafka 消息，广播到所有已注册的 asyncio.Queue。"""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "cc.transcript.realtime.v1",
        on_error: Any = None,
    ) -> None:
        self._bootstrap = bootstrap_servers
        self._topic = topic
        self._consumer: AIOKafkaConsumer | None = None
        self._subscribers: dict[str, asyncio.Queue[dict[str, Any]]] = {}
        self._task: asyncio.Task | None = None
        self._on_error = on_error

    @property
    def bootstrap_servers(self) -> str:
        return self._bootstrap

    @property
    def topic(self) -> str:
        return self._topic

    def subscribe(self) -> tuple[str, asyncio.Queue[dict[str, Any]]]:
        """注册一个订阅者，返回 (subscriber_id, queue)。"""
        sid = uuid.uuid4().hex[:8]
        q: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=50_000)
        self._subscribers[sid] = q
        return sid, q

    def unsubscribe(self, sid: str) -> None:
        self._subscribers.pop(sid, None)

    async def start(self) -> None:
        """启动 Kafka 消费循环。"""
        self._consumer = AIOKafkaConsumer(
            self._topic,
            bootstrap_servers=self._bootstrap,
            group_id=None,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda v: json.loads(v) if v else None,
            key_deserializer=lambda k: k.decode("utf-8") if k else None,
        )
        await self._consumer.start()
        self._task = asyncio.create_task(self._consume_loop())

    async def _consume_loop(self) -> None:
        assert self._consumer is not None
        try:
            async for msg in self._consumer:
                event = {
                    "topic": msg.topic,
                    "partition": msg.partition,
                    "offset": msg.offset,
                    "key": msg.key,
                    "value": msg.value,
                    "timestamp": msg.timestamp,
                }
                for sid, q in list(self._subscribers.items()):
                    try:
                        q.put_nowait(event)
                    except asyncio.QueueFull:
                        try:
                            q.get_nowait()
                        except asyncio.QueueEmpty:
                            pass
                        try:
                            q.put_nowait(event)
                        except asyncio.QueueFull:
                            pass
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            log.exception("Kafka consume loop error")
            if self._on_error:
                self._on_error(str(exc))

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._consumer:
            await self._consumer.stop()
            self._consumer = None


async def purge_topic_messages(
    bootstrap_servers: str,
    topic: str,
    *,
    timeout_ms: int = 60_000,
) -> dict[str, Any]:
    """使用 Kafka DeleteRecords API 删除 topic 内**已提交**的全部消息（按分区截断到当前 log end）。

    需 broker 支持协议版本；开发环境常用。若 topic 不存在或已无数据则返回对应状态。

    分区列表通过 Admin **describe_topics** 获取；未 assign 的 Consumer 上 ``partitions_for_topic`` 常为 ``None``，不可靠。
    """
    admin_meta = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin_meta.start()
    try:
        infos = await admin_meta.describe_topics([topic])
    finally:
        await admin_meta.close()

    if not infos:
        return {
            "status": "error",
            "error": f"无法获取 topic 元数据（集群不可达或拒绝请求）: {topic}",
        }

    meta = infos[0]
    err_code = meta.get("error_code", 0)
    if err_code:
        try:
            err_name = type(for_code(err_code)).__name__
        except Exception:
            err_name = f"error_code={err_code}"
        return {
            "status": "error",
            "error": f"topic 不可用（{err_name}）: {topic}",
        }

    partitions = meta.get("partitions") or []
    if not partitions:
        return {
            "status": "error",
            "error": f"topic 无分区（可能尚未创建）: {topic}",
        }

    tps = [TopicPartition(topic, p["partition"]) for p in partitions]

    consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        enable_auto_commit=False,
    )
    await consumer.start()
    try:
        consumer.assign(tps)
        end_map = await consumer.end_offsets(tps)
        to_delete = {
            tp: RecordsToDelete(before_offset=end_map[tp])
            for tp in tps
            if end_map[tp] > 0
        }
    finally:
        await consumer.stop()

    if not to_delete:
        return {
            "status": "ok",
            "topic": topic,
            "message": "topic 已无消息可删",
            "partitions_truncated": 0,
        }

    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        low_after = await admin.delete_records(to_delete, timeout_ms=timeout_ms)
    finally:
        await admin.close()

    return {
        "status": "ok",
        "topic": topic,
        "message": "已提交范围的消息已删除（DeleteRecords）",
        "partitions_truncated": len(to_delete),
        "low_watermark_after": {str(tp.partition): low_after.get(tp) for tp in to_delete},
    }
