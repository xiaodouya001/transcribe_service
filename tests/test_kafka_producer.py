"""coverage: producer.kafka_producer"""

from __future__ import annotations

import asyncio
import ssl
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from realtime_transcribe_service.producer import kafka_producer as kp


@pytest.mark.asyncio
async def test_ensure_topic_ignores_topic_already_exists_error():
    admin = AsyncMock()
    admin.start = AsyncMock()
    admin.create_topics = AsyncMock(side_effect=RuntimeError("topic already exists"))
    admin.close = AsyncMock()
    with patch.object(kp, "AIOKafkaAdminClient", return_value=admin):
        await kp._ensure_topic("127.0.0.1:9092", "t", 3, 1)
    admin.start.assert_awaited_once()
    admin.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_ensure_topic_raises_on_unexpected_error(monkeypatch):
    admin = AsyncMock()
    admin.start = AsyncMock()
    admin.create_topics = AsyncMock(side_effect=RuntimeError("broker down"))
    admin.close = AsyncMock()
    warn_mock = MagicMock()
    monkeypatch.setattr(kp.log, "warning", warn_mock)
    with patch.object(kp, "AIOKafkaAdminClient", return_value=admin):
        with pytest.raises(RuntimeError, match="broker down"):
            await kp._ensure_topic("127.0.0.1:9092", "t", 3, 1)
    warn_mock.assert_called_once()


def test_build_kafka_client_kwargs_for_sasl_ssl():
    kwargs = kp._build_kafka_client_kwargs(
        "broker-a:9094",
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_username="alice",
        sasl_password="secret",
    )

    assert kwargs["bootstrap_servers"] == "broker-a:9094"
    assert kwargs["security_protocol"] == "SASL_SSL"
    assert kwargs["sasl_mechanism"] == "SCRAM-SHA-512"
    assert kwargs["sasl_plain_username"] == "alice"
    assert kwargs["sasl_plain_password"] == "secret"
    assert isinstance(kwargs["ssl_context"], ssl.SSLContext)


def test_build_kafka_client_kwargs_uses_explicit_ca_file():
    ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    with patch.object(kp.ssl, "create_default_context", return_value=ssl_ctx) as create_ctx:
        kwargs = kp._build_kafka_client_kwargs(
            "broker-a:9094",
            security_protocol="SASL_SSL",
            ssl_ca_file="/tmp/msk-ca.pem",
            sasl_mechanism="SCRAM-SHA-512",
            sasl_username="alice",
            sasl_password="secret",
        )

    create_ctx.assert_called_once_with(cafile="/tmp/msk-ca.pem")
    assert kwargs["ssl_context"] is ssl_ctx


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
            bootstrap_servers="127.0.0.1:9092",
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
async def test_kafka_producer_admin_mode_passes_sasl_kwargs_to_admin_and_producer():
    ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    send_mock = AsyncMock()
    prod_mock = MagicMock()
    prod_mock.start = AsyncMock()
    prod_mock.send_and_wait = send_mock
    prod_mock.stop = AsyncMock()
    prod_mock.flush = AsyncMock()

    admin = AsyncMock()
    admin.start = AsyncMock()
    admin.create_topics = AsyncMock()
    admin.close = AsyncMock()

    with patch.object(kp.ssl, "create_default_context", return_value=ssl_ctx) as create_ctx, patch.object(
        kp, "AIOKafkaAdminClient", return_value=admin
    ) as admin_ctor, patch.object(kp, "AIOKafkaProducer", return_value=prod_mock) as producer_ctor:
        p = kp.KafkaProducer(
            bootstrap_servers="broker-a:9094",
            mode="admin",
            security_protocol="SASL_SSL",
            ssl_ca_file="/tmp/msk-ca.pem",
            sasl_mechanism="SCRAM-SHA-512",
            sasl_username="alice",
            sasl_password="secret",
        )
        await p.ensure_ready()

    create_ctx.assert_called_once_with(cafile="/tmp/msk-ca.pem")

    admin_kwargs = admin_ctor.call_args.kwargs
    producer_kwargs = producer_ctor.call_args.kwargs
    assert admin_kwargs["bootstrap_servers"] == "broker-a:9094"
    assert admin_kwargs["security_protocol"] == "SASL_SSL"
    assert admin_kwargs["sasl_mechanism"] == "SCRAM-SHA-512"
    assert admin_kwargs["sasl_plain_username"] == "alice"
    assert admin_kwargs["sasl_plain_password"] == "secret"
    assert admin_kwargs["ssl_context"] is ssl_ctx
    assert producer_kwargs["bootstrap_servers"] == "broker-a:9094"
    assert producer_kwargs["security_protocol"] == "SASL_SSL"
    assert producer_kwargs["sasl_mechanism"] == "SCRAM-SHA-512"
    assert producer_kwargs["sasl_plain_username"] == "alice"
    assert producer_kwargs["sasl_plain_password"] == "secret"
    assert producer_kwargs["ssl_context"] is ssl_ctx


@pytest.mark.asyncio
async def test_kafka_producer_aws_msk_mode_skips_topic_creation():
    prod_mock = MagicMock()
    prod_mock.start = AsyncMock()
    prod_mock.stop = AsyncMock()

    with patch.object(kp, "_ensure_topic", new=AsyncMock()) as ensure_topic, patch.object(
        kp, "AIOKafkaProducer", return_value=prod_mock
    ):
        p = kp.KafkaProducer(
            bootstrap_servers="127.0.0.1:9092",
            mode="aws_msk",
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-256",
            sasl_username="alice",
            sasl_password="secret",
        )
        await p.ensure_ready()

    ensure_topic.assert_not_awaited()


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
        p = kp.KafkaProducer(
            bootstrap_servers="127.0.0.1:9092",
            send_timeout_sec=0.01,
        )
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
        p = kp.KafkaProducer(bootstrap_servers="127.0.0.1:9092")
        await p.ensure_ready()
        with pytest.raises(RuntimeError):
            await p.send("c1", {"x": 1})


@pytest.mark.asyncio
async def test_flush_close_no_producer():
    p = kp.KafkaProducer(bootstrap_servers="127.0.0.1:9092")
    await p.flush()
    await p.close()


@pytest.mark.asyncio
async def test_ensure_ready_producer_start_fails_stops_producer():
    prod_mock = MagicMock()
    prod_mock.start = AsyncMock(side_effect=RuntimeError("start failed"))
    prod_mock.stop = AsyncMock()
    admin = AsyncMock()
    admin.start = AsyncMock()
    admin.create_topics = AsyncMock()
    admin.close = AsyncMock()
    with patch.object(kp, "AIOKafkaAdminClient", return_value=admin), patch.object(
        kp, "AIOKafkaProducer", return_value=prod_mock
    ):
        p = kp.KafkaProducer(bootstrap_servers="127.0.0.1:9092")
        with pytest.raises(RuntimeError, match="start failed"):
            await p.ensure_ready()
        prod_mock.stop.assert_awaited_once()
        assert p._producer is None


@pytest.mark.asyncio
async def test_ensure_ready_ensure_topic_raises():
    with patch.object(
        kp,
        "_ensure_topic",
        side_effect=RuntimeError("admin down"),
    ):
        p = kp.KafkaProducer(bootstrap_servers="127.0.0.1:9092")
        with pytest.raises(RuntimeError, match="admin down"):
            await p.ensure_ready()

