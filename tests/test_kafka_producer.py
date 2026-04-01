"""coverage: producer.kafka_producer, producer.kafka_connection"""

from __future__ import annotations

import asyncio
import ssl
import sys
import types
from typing import cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from realtime_transcribe_service.constants import KAFKA_MODE
from realtime_transcribe_service.producer import kafka_connection as kc
from realtime_transcribe_service.producer import kafka_producer as kp


def test_local_plaintext_connection_kwargs():
    conn = kc.LocalPlaintextKafkaConnection()
    kwargs = conn.build_client_kwargs(bootstrap_servers="127.0.0.1:9092")

    assert conn.profile_label == "local_plaintext"
    assert kwargs["bootstrap_servers"] == "127.0.0.1:9092"
    assert kwargs["security_protocol"] == "PLAINTEXT"
    assert "ssl_context" not in kwargs


def test_aws_msk_iam_connection_kwargs():
    ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    token_provider = MagicMock()
    with patch.object(kc.ssl, "create_default_context", return_value=ssl_ctx) as create_ctx, patch.object(
        kc, "MSKTokenProvider", return_value=token_provider
    ) as provider_ctor:
        conn = kc.AwsMskIamKafkaConnection(
            aws_region="ap-east-1",
            ssl_ca_file="/tmp/ca.pem",
            aws_debug_creds=True,
        )
        kwargs = conn.build_client_kwargs(
            bootstrap_servers="b-1.example.amazonaws.com:9098",
        )

    create_ctx.assert_called_once_with(cafile="/tmp/ca.pem")
    provider_ctor.assert_called_once_with("ap-east-1", aws_debug_creds=True)
    assert conn.profile_label == "aws_msk_iam"
    assert kwargs["bootstrap_servers"] == "b-1.example.amazonaws.com:9098"
    assert kwargs["security_protocol"] == "SASL_SSL"
    assert kwargs["sasl_mechanism"] == "OAUTHBEARER"
    assert kwargs["sasl_oauth_token_provider"] is token_provider
    assert kwargs["ssl_context"] is ssl_ctx


def test_kafka_connection_for_mode_aws_msk_requires_region():
    with pytest.raises(ValueError, match="aws_region is required"):
        kc.kafka_connection_for_mode("aws_msk")


def test_kafka_connection_for_mode_local():
    c = kc.kafka_connection_for_mode("local")
    assert isinstance(c, kc.LocalPlaintextKafkaConnection)


def test_kafka_connection_for_mode_msk():
    c = kc.kafka_connection_for_mode(
        "aws_msk",
        aws_region="ap-east-1",
        ssl_ca_file=None,
        aws_debug_creds=False,
    )
    assert isinstance(c, kc.AwsMskIamKafkaConnection)


def test_kafka_connection_for_mode_rejects_unknown_mode():
    with pytest.raises(ValueError, match="unsupported KAFKA_MODE"):
        kc.kafka_connection_for_mode(cast(KAFKA_MODE, "bogus"))


@pytest.mark.asyncio
async def test_msk_token_provider_refreshes_and_caches():
    with patch.object(
        kc,
        "_generate_msk_auth_token",
        side_effect=[("token-1", 9999999999999), ("token-2", 1)],
    ) as gen:
        provider = kc.MSKTokenProvider("ap-east-1", aws_debug_creds=True)
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
        token, expiry_ms = kc._generate_msk_auth_token("ap-east-1", aws_debug_creds=True)

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
            kc._generate_msk_auth_token("ap-east-1")


@pytest.mark.asyncio
async def test_kafka_producer_none_compression():
    send_mock = AsyncMock()
    prod_mock = MagicMock()
    prod_mock.start = AsyncMock()
    prod_mock.send_and_wait = send_mock
    prod_mock.flush = AsyncMock()
    prod_mock.stop = AsyncMock()

    with patch.object(kp, "AIOKafkaProducer", return_value=prod_mock):
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
async def test_kafka_producer_local_connection_passes_plaintext_to_producer():
    send_mock = AsyncMock()
    prod_mock = MagicMock()
    prod_mock.start = AsyncMock()
    prod_mock.send_and_wait = send_mock
    prod_mock.stop = AsyncMock()
    prod_mock.flush = AsyncMock()

    with patch.object(kp, "AIOKafkaProducer", return_value=prod_mock) as producer_ctor:
        p = kp.KafkaProducer(
            bootstrap_servers="127.0.0.1:9092",
            connection=kc.LocalPlaintextKafkaConnection(),
        )
        await p.ensure_ready()

    producer_kwargs = producer_ctor.call_args.kwargs
    assert producer_kwargs["bootstrap_servers"] == "127.0.0.1:9092"
    assert producer_kwargs["security_protocol"] == "PLAINTEXT"
    assert "ssl_context" not in producer_kwargs


@pytest.mark.asyncio
async def test_kafka_producer_msk_connection_uses_iam():
    prod_mock = MagicMock()
    prod_mock.start = AsyncMock()
    prod_mock.stop = AsyncMock()
    token_provider = MagicMock()

    with patch.object(kp, "AIOKafkaProducer", return_value=prod_mock) as producer_ctor, patch.object(
        kc, "MSKTokenProvider", return_value=token_provider
    ) as provider_ctor:
        p = kp.KafkaProducer(
            bootstrap_servers="b-1.example.amazonaws.com:9098",
            connection=kc.AwsMskIamKafkaConnection(
                aws_region="ap-east-1",
                aws_debug_creds=True,
            ),
        )
        await p.ensure_ready()

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
    with patch.object(kp, "AIOKafkaProducer", return_value=prod_mock):
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
    prod_mock.flush = AsyncMock()
    with patch.object(kp, "AIOKafkaProducer", return_value=prod_mock):
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
    with patch.object(kp, "AIOKafkaProducer", return_value=prod_mock):
        p = kp.KafkaProducer(bootstrap_servers="127.0.0.1:9092")
        with pytest.raises(RuntimeError, match="start failed"):
            await p.ensure_ready()
        prod_mock.stop.assert_awaited_once()
        assert p._producer is None
