"""coverage: producer.kafka_producer"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from transcribe_service.producer import kafka_producer as kp


@pytest.mark.asyncio
async def test_ensure_topic_swallows_create_error():
    admin = AsyncMock()
    admin.start = AsyncMock()
    admin.create_topics = AsyncMock(side_effect=RuntimeError("exists"))
    admin.close = AsyncMock()
    with patch.object(kp, "AIOKafkaAdminClient", return_value=admin):
        await kp._ensure_topic("localhost:9092", "t", 3, 1)
    admin.start.assert_awaited_once()
    admin.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_kafka_producer_none_compression():
    send_mock = AsyncMock()
    prod_mock = MagicMock()
    prod_mock.start = AsyncMock()
    prod_mock.send_and_wait = send_mock
    prod_mock.flush = AsyncMock()
    prod_mock.stop = AsyncMock()

    admin = AsyncMock()
    admin.start = AsyncMock()
    admin.create_topics = AsyncMock()
    admin.close = AsyncMock()

    with patch.object(kp, "AIOKafkaAdminClient", return_value=admin), patch.object(
        kp, "AIOKafkaProducer", return_value=prod_mock
    ):
        p = kp.KafkaProducer(
            compression_type="none",
            send_timeout_sec=5.0,
        )
        await p.ensure_ready()
        assert p._producer is prod_mock
        await p.send("c1", {"k": "v"})
        prod_mock.send_and_wait.assert_awaited()
        send_mock.assert_awaited_once()

        await p.flush()
        await p.close()
        prod_mock.flush.assert_awaited()
        prod_mock.stop.assert_awaited()


@pytest.mark.asyncio
async def test_kafka_send_timeout():
    prod_mock = MagicMock()
    prod_mock.start = AsyncMock()
    prod_mock.send_and_wait = AsyncMock(side_effect=asyncio.TimeoutError())
    prod_mock.flush = AsyncMock()
    admin = AsyncMock()
    admin.start = AsyncMock()
    admin.create_topics = AsyncMock()
    admin.close = AsyncMock()
    with patch.object(kp, "AIOKafkaAdminClient", return_value=admin), patch.object(
        kp, "AIOKafkaProducer", return_value=prod_mock
    ):
        p = kp.KafkaProducer(send_timeout_sec=0.01)
        await p.ensure_ready()
        with pytest.raises(asyncio.TimeoutError):
            await p.send("c1", {"x": 1})


@pytest.mark.asyncio
async def test_kafka_send_failure():
    prod_mock = MagicMock()
    prod_mock.start = AsyncMock()
    prod_mock.send_and_wait = AsyncMock(side_effect=RuntimeError("broker"))
    admin = AsyncMock()
    admin.start = AsyncMock()
    admin.create_topics = AsyncMock()
    admin.close = AsyncMock()
    with patch.object(kp, "AIOKafkaAdminClient", return_value=admin), patch.object(
        kp, "AIOKafkaProducer", return_value=prod_mock
    ):
        p = kp.KafkaProducer()
        await p.ensure_ready()
        with pytest.raises(RuntimeError):
            await p.send("c1", {"x": 1})


@pytest.mark.asyncio
async def test_flush_close_no_producer():
    p = kp.KafkaProducer()
    await p.flush()
    await p.close()
