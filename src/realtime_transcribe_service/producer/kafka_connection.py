"""Kafka broker connection profiles for aiokafka — two concrete profiles behind one type alias.

- **Local plaintext** (:class:`LocalPlaintextKafkaConnection`): PLAINTEXT only; for local
  docker-compose. Does not create topics.
- **AWS MSK IAM** (:class:`AwsMskIamKafkaConnection`): ``SASL_SSL`` + ``OAUTHBEARER`` via
  ``aws-msk-iam-sasl-signer-python``.

:class:`~realtime_transcribe_service.producer.kafka_producer.KafkaProducer` accepts
:data:`KafkaBrokerConnection` (``LocalPlaintextKafkaConnection | AwsMskIamKafkaConnection``), not
``KAFKA_MODE`` branching inside the producer.
"""

from __future__ import annotations

import asyncio
import ssl
import threading
import time
from dataclasses import dataclass
from typing import Any, TypeAlias

from aiokafka.abc import AbstractTokenProvider

from realtime_transcribe_service.constants import KAFKA_MODE


def _generate_msk_auth_token(region: str, *, aws_debug_creds: bool = False) -> tuple[str, int]:
    """Generate an MSK IAM auth token using the default AWS credential chain."""
    try:
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider  # pyright: ignore[reportMissingImports]
    except ImportError as exc:  # pragma: no cover - exercised when dependency is missing
        raise RuntimeError(
            "aws-msk-iam-sasl-signer-python is required for AwsMskIamKafkaConnection"
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


@dataclass(frozen=True, slots=True)
class LocalPlaintextKafkaConnection:
    """PLAINTEXT bootstrap to a local or non-TLS broker (dev / docker-compose)."""

    @property
    def profile_label(self) -> str:
        return "local_plaintext"

    def build_client_kwargs(self, *, bootstrap_servers: str) -> dict[str, Any]:
        return {
            "bootstrap_servers": bootstrap_servers,
            "security_protocol": "PLAINTEXT",
        }


@dataclass(frozen=True, slots=True)
class AwsMskIamKafkaConnection:
    """MSK with TLS and OAUTHBEARER tokens from the MSK IAM signer."""

    aws_region: str
    ssl_ca_file: str | None = None
    aws_debug_creds: bool = False

    @property
    def profile_label(self) -> str:
        return "aws_msk_iam"

    def build_client_kwargs(self, *, bootstrap_servers: str) -> dict[str, Any]:
        return {
            "bootstrap_servers": bootstrap_servers,
            "security_protocol": "SASL_SSL",
            "ssl_context": ssl.create_default_context(cafile=self.ssl_ca_file),
            "sasl_mechanism": "OAUTHBEARER",
            "sasl_oauth_token_provider": MSKTokenProvider(
                self.aws_region,
                aws_debug_creds=self.aws_debug_creds,
            ),
        }


KafkaBrokerConnection: TypeAlias = LocalPlaintextKafkaConnection | AwsMskIamKafkaConnection


def kafka_connection_for_mode(
    mode: KAFKA_MODE,
    *,
    aws_region: str | None = None,
    ssl_ca_file: str | None = None,
    aws_debug_creds: bool = False,
) -> KafkaBrokerConnection:
    """Map ``KAFKA_MODE`` env to a concrete :data:`KafkaBrokerConnection` profile."""
    if mode == "local":
        return LocalPlaintextKafkaConnection()
    if mode == "aws_msk":
        if aws_region is None:
            raise ValueError("aws_region is required for KAFKA_MODE=aws_msk")
        return AwsMskIamKafkaConnection(
            aws_region=aws_region,
            ssl_ca_file=ssl_ca_file,
            aws_debug_creds=aws_debug_creds,
        )
    raise ValueError(f"unsupported KAFKA_MODE: {mode!r}")
