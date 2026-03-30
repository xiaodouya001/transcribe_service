"""Kafka outbound message converter implementation."""

from __future__ import annotations

from datetime import datetime, timezone

from realtime_transcribe_service.converter.protocols import KafkaMessageConverterBackend
from realtime_transcribe_service.schemas.request import InboundMessage
from realtime_transcribe_service.utils.timestamp import format_utc_timestamp


class KafkaMessageConverter(KafkaMessageConverterBackend):
    """Assemble and validate Kafka outbound payload with enrich fields."""

    def to_kafka_payload(self, msg: InboundMessage, raw_request: dict) -> dict:
        # Reuse already-validated metaData/payload (InboundMessage) directly.
        # Avoid wrapping them in another Pydantic model on the hot path.
        # raw_request is retained for API symmetry and tests that assert it is not mutated.
        _ = raw_request
        produced_at = datetime.now(timezone.utc)
        return {
            "metaData": msg.metaData.model_dump(mode="json"),
            "payload": msg.payload.model_dump(mode="json"),
            "enrich": {
                # Canonical millisecond UTC string, aligned with ACK/ERROR builders.
                "eventProduceTimestamp": format_utc_timestamp(produced_at),
            },
        }
