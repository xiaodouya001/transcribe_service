"""Tests for schemas 契约层。"""

import pytest
from pydantic import ValidationError

from transcribe_service.schemas.request import EventType, InboundMessage, Speaker
from transcribe_service.schemas.response import build_ack, build_error


class TestInboundMessage:
    """InboundMessage 校验测试。"""

    def test_valid_ongoing(self, valid_ongoing_msg: dict):
        msg = InboundMessage.model_validate(valid_ongoing_msg)
        assert msg.metaData.eventType == EventType.SESSION_ONGOING
        assert msg.payload.sequenceNumber == 0
        assert msg.payload.speaker == Speaker.AGENT

    def test_valid_complete(self, valid_complete_msg: dict):
        msg = InboundMessage.model_validate(valid_complete_msg)
        assert msg.metaData.eventType == EventType.SESSION_COMPLETE
        assert msg.metaData.callEndTimeStamp is not None

    def test_ongoing_with_call_end_timestamp_fails(self, valid_ongoing_msg: dict):
        valid_ongoing_msg["metaData"]["callEndTimeStamp"] = "2025-03-21T10:45:00.000Z"
        with pytest.raises(ValidationError, match="callEndTimeStamp must be null"):
            InboundMessage.model_validate(valid_ongoing_msg)

    def test_complete_without_call_end_timestamp_fails(self, valid_complete_msg: dict):
        valid_complete_msg["metaData"]["callEndTimeStamp"] = None
        with pytest.raises(ValidationError, match="callEndTimeStamp must be provided"):
            InboundMessage.model_validate(valid_complete_msg)

    def test_is_final_false_fails(self, valid_ongoing_msg: dict):
        valid_ongoing_msg["payload"]["isFinal"] = False
        with pytest.raises(ValidationError, match="isFinal must be true"):
            InboundMessage.model_validate(valid_ongoing_msg)

    def test_missing_conversation_id_fails(self, valid_ongoing_msg: dict):
        del valid_ongoing_msg["metaData"]["conversationId"]
        with pytest.raises(ValidationError):
            InboundMessage.model_validate(valid_ongoing_msg)

    def test_invalid_event_type_fails(self, valid_ongoing_msg: dict):
        valid_ongoing_msg["metaData"]["eventType"] = "INVALID"
        with pytest.raises(ValidationError):
            InboundMessage.model_validate(valid_ongoing_msg)

    def test_invalid_speaker_fails(self, valid_ongoing_msg: dict):
        valid_ongoing_msg["payload"]["speaker"] = "Bot"
        with pytest.raises(ValidationError):
            InboundMessage.model_validate(valid_ongoing_msg)

    def test_negative_sequence_number_fails(self, valid_ongoing_msg: dict):
        valid_ongoing_msg["payload"]["sequenceNumber"] = -1
        with pytest.raises(ValidationError):
            InboundMessage.model_validate(valid_ongoing_msg)

    def test_invalid_timestamp_format_fails(self, valid_ongoing_msg: dict):
        valid_ongoing_msg["metaData"]["callStartTimeStamp"] = "bad-ts"
        with pytest.raises(ValidationError):
            InboundMessage.model_validate(valid_ongoing_msg)

    def test_non_utc_timestamp_fails(self, valid_ongoing_msg: dict):
        valid_ongoing_msg["payload"]["createdAtTimeStamp"] = "2025-03-21T18:32:20.000+08:00"
        with pytest.raises(ValidationError):
            InboundMessage.model_validate(valid_ongoing_msg)

    def test_non_utc_metadata_timestamp_fails(self, valid_ongoing_msg: dict):
        valid_ongoing_msg["metaData"]["callStartTimeStamp"] = "2025-03-21T18:30:02.327+08:00"
        with pytest.raises(ValidationError):
            InboundMessage.model_validate(valid_ongoing_msg)


class TestBuildAck:
    def test_ack_structure(self, conversation_id: str):
        ack = build_ack(conversation_id, 5)
        assert ack["metaData"]["conversationId"] == conversation_id
        assert ack["metaData"]["eventType"] == "TRANSCRIPT_ACK"
        assert ack["payload"]["sequenceNumber"] == 5
        assert "createdAtTimeStamp" in ack["payload"]


class TestBuildError:
    def test_error_structure(self, conversation_id: str):
        err = build_error(conversation_id, "E1006", "Out of order", "seq=5 unexpected")
        assert err["metaData"]["eventType"] == "ERROR"
        assert err["error"]["code"] == "E1006"
        assert err["error"]["details"] == "seq=5 unexpected"

    def test_error_without_details(self, conversation_id: str):
        err = build_error(conversation_id, "E1007", "Internal error")
        assert err["error"]["details"] is None
