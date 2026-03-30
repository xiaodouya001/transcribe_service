"""Tests for Kafka outbound schema and converter layer."""

from __future__ import annotations

import copy
import re

from realtime_transcribe_service.converter.kafka_message_converter import KafkaMessageConverter
from realtime_transcribe_service.schemas.request import InboundMessage


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

    def test_converted_payload_has_expected_kafka_shape(self, valid_ongoing_msg: dict):
        converter = KafkaMessageConverter()
        msg = InboundMessage.model_validate(valid_ongoing_msg)

        converted = converter.to_kafka_payload(msg, valid_ongoing_msg)
        assert set(converted.keys()) == {"metaData", "payload", "enrich"}
        roundtrip = InboundMessage.model_validate(
            {"metaData": converted["metaData"], "payload": converted["payload"]}
        )
        assert roundtrip == msg
        assert set(converted["enrich"].keys()) == {"eventProduceTimestamp"}
        assert self._TS_PATTERN.match(converted["enrich"]["eventProduceTimestamp"])

    def test_existing_enrich_dict_is_copied_and_overwritten(self, valid_ongoing_msg: dict):
        converter = KafkaMessageConverter()
        source = copy.deepcopy(valid_ongoing_msg)
        source["enrich"] = {"eventProduceTimestamp": "2020-01-01T00:00:00.000Z"}
        msg = InboundMessage.model_validate(valid_ongoing_msg)

        converted = converter.to_kafka_payload(msg, source)
        assert source["enrich"]["eventProduceTimestamp"] == "2020-01-01T00:00:00.000Z"
        assert converted["enrich"]["eventProduceTimestamp"] != source["enrich"]["eventProduceTimestamp"]
