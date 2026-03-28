"""Kafka outbound message converter implementation."""

from __future__ import annotations

from datetime import datetime, timezone

from realtime_transcribe_service.converter.protocols import KafkaMessageConverterBackend
from realtime_transcribe_service.schemas.kafka_outbound import KafkaEnrich, KafkaOutboundMessage
from realtime_transcribe_service.schemas.request import InboundMessage
from realtime_transcribe_service.utils.timestamp import format_utc_timestamp


class KafkaMessageConverter(KafkaMessageConverterBackend):
    """Assemble and validate Kafka outbound payload with enrich fields."""

    def to_kafka_payload(self, msg: InboundMessage, raw_request: dict) -> dict:
        # Reuse already-validated metaData/payload (InboundMessage); avoid re-parsing raw JSON.
        # raw_request is retained for API symmetry and tests that assert it is not mutated.
        _ = raw_request
        produced_at = datetime.now(timezone.utc)
        outbound = KafkaOutboundMessage(
            metaData=msg.metaData,
            payload=msg.payload,
            enrich=KafkaEnrich(eventProduceTimestamp=produced_at),
        )
        payload = outbound.model_dump(mode="json")
        # Canonical millisecond UTC string (aligned with ACK/ERROR builders).
        payload["enrich"]["eventProduceTimestamp"] = format_utc_timestamp(produced_at)
        return payload
