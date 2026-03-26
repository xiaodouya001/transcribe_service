"""Tests for Kafka outbound schema and converter layer."""

from __future__ import annotations

import copy
import re

import pytest
from pydantic import ValidationError

from realtime_transcribe_service.converter.kafka_message_converter import KafkaMessageConverter
from realtime_transcribe_service.schemas.kafka_outbound import KafkaOutboundMessage
from realtime_transcribe_service.schemas.request import InboundMessage


class TestKafkaOutboundSchema:
    def test_valid_outbound_message_passes(self, valid_ongoing_msg: dict):
        payload = {
            **valid_ongoing_msg,
            "enrich": {"eventProduceTimestamp": "2026-03-27T10:11:12.345Z"},
        }
        model = KafkaOutboundMessage.model_validate(payload)
        assert model.enrich.eventProduceTimestamp.isoformat().startswith("2026-03-27T10:11:12.345")

    def test_missing_enrich_fails(self, valid_ongoing_msg: dict):
        with pytest.raises(ValidationError):
            KafkaOutboundMessage.model_validate(valid_ongoing_msg)

    def test_missing_event_produce_timestamp_fails(self, valid_ongoing_msg: dict):
        payload = {**valid_ongoing_msg, "enrich": {}}
        with pytest.raises(ValidationError):
            KafkaOutboundMessage.model_validate(payload)

    def test_non_utc_timestamp_fails(self, valid_ongoing_msg: dict):
        payload = {
            **valid_ongoing_msg,
            "enrich": {"eventProduceTimestamp": "2026-03-27T18:11:12.345+08:00"},
        }
        with pytest.raises(ValidationError):
            KafkaOutboundMessage.model_validate(payload)

    def test_malformed_timestamp_fails(self, valid_ongoing_msg: dict):
        payload = {
            **valid_ongoing_msg,
            "enrich": {"eventProduceTimestamp": "bad-timestamp"},
        }
        with pytest.raises(ValidationError):
            KafkaOutboundMessage.model_validate(payload)

    def test_unexpected_extra_fields_in_enrich_fail(self, valid_ongoing_msg: dict):
        payload = {
            **valid_ongoing_msg,
            "enrich": {
                "eventProduceTimestamp": "2026-03-27T10:11:12.345Z",
                "unexpected": "x",
            },
        }
        with pytest.raises(ValidationError):
            KafkaOutboundMessage.model_validate(payload)


class TestKafkaMessageConverter:
    _TS_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")

    def test_original_request_not_mutated(self, valid_ongoing_msg: dict):
        converter = KafkaMessageConverter()
        msg = InboundMessage.model_validate(valid_ongoing_msg)
        source = copy.deepcopy(valid_ongoing_msg)
        snapshot = copy.deepcopy(source)

        _ = converter.to_kafka_payload(msg, source)
        assert source == snapshot
        assert "enrich" not in source

    def test_output_contains_original_and_enrich_field(self, valid_ongoing_msg: dict):
        converter = KafkaMessageConverter()
        msg = InboundMessage.model_validate(valid_ongoing_msg)

        converted = converter.to_kafka_payload(msg, valid_ongoing_msg)
        roundtrip = InboundMessage.model_validate(
            {"metaData": converted["metaData"], "payload": converted["payload"]}
        )
        assert roundtrip == msg
        assert "enrich" in converted
        assert "eventProduceTimestamp" in converted["enrich"]

    def test_generated_timestamp_matches_canonical_format(self, valid_ongoing_msg: dict):
        converter = KafkaMessageConverter()
        msg = InboundMessage.model_validate(valid_ongoing_msg)

        converted = converter.to_kafka_payload(msg, valid_ongoing_msg)
        ts = converted["enrich"]["eventProduceTimestamp"]
        assert self._TS_PATTERN.match(ts)

    def test_converted_payload_validates_as_kafka_outbound(self, valid_ongoing_msg: dict):
        converter = KafkaMessageConverter()
        msg = InboundMessage.model_validate(valid_ongoing_msg)

        converted = converter.to_kafka_payload(msg, valid_ongoing_msg)
        validated = KafkaOutboundMessage.model_validate(converted)
        assert validated.metaData.conversationId == valid_ongoing_msg["metaData"]["conversationId"]

    def test_existing_enrich_dict_is_copied_and_overwritten(self, valid_ongoing_msg: dict):
        converter = KafkaMessageConverter()
        source = copy.deepcopy(valid_ongoing_msg)
        source["enrich"] = {"eventProduceTimestamp": "2020-01-01T00:00:00.000Z"}
        msg = InboundMessage.model_validate(valid_ongoing_msg)

        converted = converter.to_kafka_payload(msg, source)
        assert source["enrich"]["eventProduceTimestamp"] == "2020-01-01T00:00:00.000Z"
        assert converted["enrich"]["eventProduceTimestamp"] != source["enrich"]["eventProduceTimestamp"]
