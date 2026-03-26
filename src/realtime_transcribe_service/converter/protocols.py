"""Converter protocols."""

from __future__ import annotations

from typing import Protocol

from realtime_transcribe_service.schemas.request import InboundMessage


class KafkaMessageConverterBackend(Protocol):
    """Convert validated inbound message to Kafka outbound payload."""

    def to_kafka_payload(self, msg: InboundMessage, raw_request: dict) -> dict:
        ...  # pragma: no cover
