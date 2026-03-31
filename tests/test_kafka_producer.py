"""coverage: producer.kafka_producer"""

from __future__ import annotations

import asyncio
import ssl
import sys
import types
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


def test_build_kafka_client_kwargs_for_admin_is_plaintext_only():
    kwargs = kp._build_kafka_client_kwargs(
        "127.0.0.1:9092",
        mode="admin",
    )

    assert kwargs["bootstrap_servers"] == "127.0.0.1:9092"
    assert kwargs["security_protocol"] == "PLAINTEXT"
    assert "ssl_context" not in kwargs


def test_build_kafka_client_kwargs_for_aws_msk_iam():
    ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    token_provider = MagicMock()
    with patch.object(kp.ssl, "create_default_context", return_value=ssl_ctx) as create_ctx, patch.object(
        kp, "MSKTokenProvider", return_value=token_provider
    ) as provider_ctor:
        kwargs = kp._build_kafka_client_kwargs(
            "b-1.example.amazonaws.com:9098",
            mode="aws_msk",
            ssl_ca_file="/tmp/ca.pem",
            aws_region="ap-east-1",
            aws_debug_creds=True,
        )

    create_ctx.assert_called_once_with(cafile="/tmp/ca.pem")
    provider_ctor.assert_called_once_with("ap-east-1", aws_debug_creds=True)
    assert kwargs["bootstrap_servers"] == "b-1.example.amazonaws.com:9098"
    assert kwargs["security_protocol"] == "SASL_SSL"
    assert kwargs["sasl_mechanism"] == "OAUTHBEARER"
    assert kwargs["sasl_oauth_token_provider"] is token_provider
    assert kwargs["ssl_context"] is ssl_ctx


def test_build_kafka_client_kwargs_for_aws_msk_requires_region():
    with pytest.raises(ValueError, match="aws_region is required"):
        kp._build_kafka_client_kwargs(
            "b-1.example.amazonaws.com:9098",
            mode="aws_msk",
        )


@pytest.mark.asyncio
async def test_msk_token_provider_refreshes_and_caches():
    with patch.object(
        kp,
        "_generate_msk_auth_token",
        side_effect=[("token-1", 9999999999999), ("token-2", 1)],
    ) as gen:
        provider = kp.MSKTokenProvider("ap-east-1", aws_debug_creds=True)
        token_1 = await provider.token()
        token_2 = await provider.token()

    assert token_1 == "token-1"
    assert token_2 == "token-1"
    gen.assert_called_once_with("ap-east-1", aws_debug_creds=True)


def test_generate_msk_auth_token_success():
    signer_module = types.SimpleNamespace(
        MSKAuthTokenProvider=types.SimpleNamespace(
            generate_auth_token=MagicMock(return_value=("token-1", 1234567890))
        )
    )
    with patch.dict(sys.modules, {"aws_msk_iam_sasl_signer": signer_module}):
        token, expiry_ms = kp._generate_msk_auth_token("ap-east-1", aws_debug_creds=True)

    assert token == "token-1"
    assert expiry_ms == 1234567890
    signer_module.MSKAuthTokenProvider.generate_auth_token.assert_called_once_with(
        "ap-east-1",
        aws_debug_creds=True,
    )


def test_generate_msk_auth_token_import_error():
    import_error = ImportError("missing signer")
    real_import = __import__

    def fail_signer_import(name, *args, **kwargs):
        if name == "aws_msk_iam_sasl_signer":
            raise import_error
        return real_import(name, *args, **kwargs)

    with patch("builtins.__import__", side_effect=fail_signer_import):
        with pytest.raises(RuntimeError, match="aws-msk-iam-sasl-signer-python"):
            kp._generate_msk_auth_token("ap-east-1")


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
async def test_kafka_producer_admin_mode_passes_plaintext_to_admin_and_producer():
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

    with patch.object(kp, "AIOKafkaAdminClient", return_value=admin) as admin_ctor, patch.object(
        kp, "AIOKafkaProducer", return_value=prod_mock
    ) as producer_ctor:
        p = kp.KafkaProducer(
            bootstrap_servers="127.0.0.1:9092",
            mode="admin",
        )
        await p.ensure_ready()

    admin_kwargs = admin_ctor.call_args.kwargs
    producer_kwargs = producer_ctor.call_args.kwargs
    assert admin_kwargs["bootstrap_servers"] == "127.0.0.1:9092"
    assert admin_kwargs["security_protocol"] == "PLAINTEXT"
    assert "ssl_context" not in admin_kwargs
    assert producer_kwargs["bootstrap_servers"] == "127.0.0.1:9092"
    assert producer_kwargs["security_protocol"] == "PLAINTEXT"
    assert "ssl_context" not in producer_kwargs


@pytest.mark.asyncio
async def test_kafka_producer_aws_msk_mode_uses_iam_and_skips_topic_creation():
    prod_mock = MagicMock()
    prod_mock.start = AsyncMock()
    prod_mock.stop = AsyncMock()
    token_provider = MagicMock()

    with patch.object(kp, "_ensure_topic", new=AsyncMock()) as ensure_topic, patch.object(
        kp, "AIOKafkaProducer", return_value=prod_mock
    ) as producer_ctor, patch.object(kp, "MSKTokenProvider", return_value=token_provider) as provider_ctor:
        p = kp.KafkaProducer(
            bootstrap_servers="b-1.example.amazonaws.com:9098",
            mode="aws_msk",
            aws_region="ap-east-1",
            aws_debug_creds=True,
        )
        await p.ensure_ready()

    ensure_topic.assert_not_awaited()
    provider_ctor.assert_called_once_with("ap-east-1", aws_debug_creds=True)
    producer_kwargs = producer_ctor.call_args.kwargs
    assert producer_kwargs["security_protocol"] == "SASL_SSL"
    assert producer_kwargs["sasl_mechanism"] == "OAUTHBEARER"
    assert producer_kwargs["sasl_oauth_token_provider"] is token_provider


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

