"""Opt-in real Kafka connectivity check driven by environment variables."""

from __future__ import annotations

import os
import traceback
from datetime import datetime, timezone
from typing import cast
from uuid import uuid4

import pytest

from realtime_transcribe_service.constants import KAFKA_MODE
from realtime_transcribe_service.producer.kafka_connection import kafka_connection_for_mode
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


@pytest.mark.asyncio
async def test_real_kafka_connectivity_from_env():
    """Validate connectivity to a real Kafka environment when explicitly enabled.

    Required environment variables when enabled:
    - RUN_REAL_KAFKA_TEST=true
    - REAL_KAFKA_BOOTSTRAP_SERVERS=<host:port[,host:port...]>

    Optional environment variables:
    - REAL_KAFKA_MODE=local|aws_msk               (default: local, for local docker-compose PLAINTEXT)
    - REAL_KAFKA_TOPIC=<topic-name>               (default: RT_KAFKA_CONNECTIVITY_TEST)
    - REAL_KAFKA_SSL_CA_FILE=<path-to-ca-bundle>  (aws_msk only, when broker chain is non-public)
    - REAL_KAFKA_AWS_REGION=<aws-region>          (required when mode=aws_msk)
    - REAL_KAFKA_AWS_DEBUG_CREDS=true|false       (default: false)
    - REAL_KAFKA_ASSERT_SEND=true|false           (default: false)

    When REAL_KAFKA_ASSERT_SEND=true the test also sends one probe message to REAL_KAFKA_TOPIC.
    """
    if not _env_flag("RUN_REAL_KAFKA_TEST"):
        pytest.skip("Set RUN_REAL_KAFKA_TEST=true to validate connectivity against a real Kafka cluster.")

    bootstrap_servers = _require_env("REAL_KAFKA_BOOTSTRAP_SERVERS")
    mode_raw = os.getenv("REAL_KAFKA_MODE", "local").strip().lower()
    if mode_raw not in ("local", "aws_msk"):
        pytest.fail(f"REAL_KAFKA_MODE must be local or aws_msk, got {mode_raw!r}")
    mode = cast(KAFKA_MODE, mode_raw)
    topic = os.getenv("REAL_KAFKA_TOPIC", "RT_KAFKA_CONNECTIVITY_TEST").strip()
    ssl_ca_file = os.getenv("REAL_KAFKA_SSL_CA_FILE")
    aws_region = os.getenv("REAL_KAFKA_AWS_REGION")
    aws_debug_creds = _env_flag("REAL_KAFKA_AWS_DEBUG_CREDS")
    assert_send = _env_flag("REAL_KAFKA_ASSERT_SEND")
    debug_summary = {
        "bootstrap_servers": bootstrap_servers,
        "mode": mode,
        "topic": topic,
        "ssl_ca_file": ssl_ca_file,
        "aws_region": aws_region,
        "aws_debug_creds": aws_debug_creds,
        "assert_send": assert_send,
    }

    if mode == "aws_msk":
        aws_region = _require_env("REAL_KAFKA_AWS_REGION")
        debug_summary["aws_region"] = aws_region

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        connection=kafka_connection_for_mode(
            mode,
            aws_region=aws_region,
            ssl_ca_file=ssl_ca_file,
            aws_debug_creds=aws_debug_creds,
        ),
        compression_type="none",
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
