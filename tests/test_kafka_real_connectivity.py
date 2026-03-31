"""Opt-in real Kafka connectivity check driven by environment variables."""

from __future__ import annotations

import os
import traceback
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from realtime_transcribe_service.producer.kafka_producer import KafkaProducer


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if value is None or not value.strip():
        pytest.fail(f"Environment variable {name} is required when RUN_REAL_KAFKA_TEST=true")
    return value.strip()


def _redact_secret(value: str | None) -> str | None:
    if value is None:
        return None
    if len(value) <= 4:
        return "*" * len(value)
    return f"{value[:2]}***{value[-2:]}"


@pytest.mark.asyncio
async def test_real_kafka_connectivity_from_env():
    """Validate connectivity to a real Kafka environment when explicitly enabled.

    Required environment variables when enabled:
    - RUN_REAL_KAFKA_TEST=true
    - REAL_KAFKA_BOOTSTRAP_SERVERS=<host:port[,host:port...]>

    Optional environment variables:
    - REAL_KAFKA_MODE=admin|aws_msk                (default: aws_msk)
    - REAL_KAFKA_TOPIC=<topic-name>               (default: RT_KAFKA_CONNECTIVITY_TEST)
    - REAL_KAFKA_SECURITY_PROTOCOL=...            (default: PLAINTEXT)
    - REAL_KAFKA_SSL_CA_FILE=<path-to-ca-bundle>
    - REAL_KAFKA_SASL_MECHANISM=SCRAM-SHA-256|SCRAM-SHA-512
    - REAL_KAFKA_SASL_USERNAME=...
    - REAL_KAFKA_SASL_PASSWORD=...
    - REAL_KAFKA_ASSERT_SEND=true|false           (default: false)

    When REAL_KAFKA_ASSERT_SEND=true the test also sends one probe message to REAL_KAFKA_TOPIC.
    """
    if not _env_flag("RUN_REAL_KAFKA_TEST"):
        pytest.skip("Set RUN_REAL_KAFKA_TEST=true to validate connectivity against a real Kafka cluster.")

    bootstrap_servers = _require_env("REAL_KAFKA_BOOTSTRAP_SERVERS")
    mode = os.getenv("REAL_KAFKA_MODE", "aws_msk").strip().lower()
    topic = os.getenv("REAL_KAFKA_TOPIC", "RT_KAFKA_CONNECTIVITY_TEST").strip()
    security_protocol = os.getenv("REAL_KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").strip().upper()
    ssl_ca_file = os.getenv("REAL_KAFKA_SSL_CA_FILE")
    sasl_mechanism = os.getenv("REAL_KAFKA_SASL_MECHANISM")
    sasl_username = os.getenv("REAL_KAFKA_SASL_USERNAME")
    sasl_password = os.getenv("REAL_KAFKA_SASL_PASSWORD")
    assert_send = _env_flag("REAL_KAFKA_ASSERT_SEND")
    debug_summary = {
        "bootstrap_servers": bootstrap_servers,
        "mode": mode,
        "topic": topic,
        "security_protocol": security_protocol,
        "ssl_ca_file": ssl_ca_file,
        "sasl_mechanism": sasl_mechanism,
        "sasl_username": _redact_secret(sasl_username),
        "assert_send": assert_send,
    }

    if security_protocol.startswith("SASL_"):
        sasl_mechanism = _require_env("REAL_KAFKA_SASL_MECHANISM")
        sasl_username = _require_env("REAL_KAFKA_SASL_USERNAME")
        sasl_password = _require_env("REAL_KAFKA_SASL_PASSWORD")
        debug_summary["sasl_mechanism"] = sasl_mechanism
        debug_summary["sasl_username"] = _redact_secret(sasl_username)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        mode=mode,
        compression_type="none",
        security_protocol=security_protocol,
        ssl_ca_file=ssl_ca_file,
        sasl_mechanism=sasl_mechanism,
        sasl_username=sasl_username,
        sasl_password=sasl_password,
        send_timeout_sec=10.0,
        linger_ms=0,
        batch_size=16384,
    )

    try:
        try:
            await producer.ensure_ready()

            if assert_send:
                await producer.send(
                    conversation_id=f"rt-connectivity-{uuid4()}",
                    payload={
                        "probe": "kafka-connectivity",
                        "source": "pytest",
                        "sentAt": datetime.now(timezone.utc).isoformat(),
                    },
                )
        except Exception as exc:  # pragma: no cover - exercised only against real environments
            tb = traceback.format_exc()
            pytest.fail(
                "Real Kafka connectivity check failed.\n"
                f"Config: {debug_summary}\n"
                f"Exception: {type(exc).__name__}: {exc}\n"
                f"Traceback:\n{tb}"
            )
    finally:
        await producer.close()
