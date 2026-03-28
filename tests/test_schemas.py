"""Tests for the schema contract layer."""

import pytest
from pydantic import ValidationError

from realtime_transcribe_service.schemas.events import EventType, ResponseEventType, Speaker
from realtime_transcribe_service.schemas.request import InboundMessage
from realtime_transcribe_service.schemas.response import (
    build_eol_ack,
    build_error,
    build_transcript_ack,
)


class TestInboundMessage:
    """InboundMessage validation tests."""

    def test_valid_ongoing(self, valid_ongoing_msg: dict):
        msg = InboundMessage.model_validate(valid_ongoing_msg)
        assert msg.metaData.eventType == EventType.SESSION_ONGOING
        assert msg.payload.sequenceNumber == 0
        assert msg.payload.speaker == Speaker.AGENT
        assert msg.payload.agentId == "3210001"
        assert msg.payload.customerId is None

    def test_valid_complete(self, valid_complete_msg: dict):
        msg = InboundMessage.model_validate(valid_complete_msg)
        assert msg.metaData.eventType == EventType.SESSION_COMPLETE
        assert msg.metaData.callEndTimeStamp is not None
        assert msg.payload.speaker == Speaker.SYSTEM

    def test_complete_with_non_eol_transcript_still_valid(self, valid_complete_msg: dict):
        valid_complete_msg["payload"]["transcript"] = "goodbye and close"
        msg = InboundMessage.model_validate(valid_complete_msg)
        assert msg.payload.transcript == "goodbye and close"

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

    def test_complete_with_non_system_speaker_fails(self, valid_complete_msg: dict):
        valid_complete_msg["payload"]["speaker"] = "Agent"
        valid_complete_msg["payload"]["agentId"] = "3210001"
        with pytest.raises(ValidationError, match="speaker must be System"):
            InboundMessage.model_validate(valid_complete_msg)

    def test_ongoing_with_system_speaker_fails(self, valid_ongoing_msg: dict):
        valid_ongoing_msg["payload"]["speaker"] = "System"
        valid_ongoing_msg["payload"]["agentId"] = None
        with pytest.raises(ValidationError, match="speaker must be Agent or Customer"):
            InboundMessage.model_validate(valid_ongoing_msg)

    def test_agent_without_agent_id_fails(self, valid_ongoing_msg: dict):
        valid_ongoing_msg["payload"]["agentId"] = None
        with pytest.raises(ValidationError, match="agentId must be provided"):
            InboundMessage.model_validate(valid_ongoing_msg)

    def test_blank_agent_id_fails(self, valid_ongoing_msg: dict):
        valid_ongoing_msg["payload"]["agentId"] = "   "
        with pytest.raises(ValidationError, match="identifier must not be empty"):
            InboundMessage.model_validate(valid_ongoing_msg)

    def test_customer_without_customer_id_fails(self, valid_ongoing_msg: dict):
        valid_ongoing_msg["payload"]["speaker"] = "Customer"
        valid_ongoing_msg["payload"]["agentId"] = None
        valid_ongoing_msg["payload"]["customerId"] = None
        with pytest.raises(ValidationError, match="customerId must be provided"):
            InboundMessage.model_validate(valid_ongoing_msg)

    def test_agent_with_customer_id_fails(self, valid_ongoing_msg: dict):
        valid_ongoing_msg["payload"]["customerId"] = "12345678"
        with pytest.raises(ValidationError, match="customerId must be null or omitted"):
            InboundMessage.model_validate(valid_ongoing_msg)

    def test_customer_with_agent_id_fails(self, valid_ongoing_msg: dict):
        valid_ongoing_msg["payload"]["speaker"] = "Customer"
        valid_ongoing_msg["payload"]["customerId"] = "12345678"
        with pytest.raises(ValidationError, match="agentId must be null or omitted"):
            InboundMessage.model_validate(valid_ongoing_msg)

    def test_customer_with_missing_non_applicable_agent_id_is_valid(self, valid_ongoing_msg: dict):
        valid_ongoing_msg["payload"]["speaker"] = "Customer"
        valid_ongoing_msg["payload"]["customerId"] = "12345678"
        del valid_ongoing_msg["payload"]["agentId"]
        msg = InboundMessage.model_validate(valid_ongoing_msg)
        assert msg.payload.speaker == Speaker.CUSTOMER
        assert msg.payload.agentId is None
        assert msg.payload.customerId == "12345678"

    def test_agent_with_missing_non_applicable_customer_id_is_valid(self, valid_ongoing_msg: dict):
        del valid_ongoing_msg["payload"]["customerId"]
        msg = InboundMessage.model_validate(valid_ongoing_msg)
        assert msg.payload.speaker == Speaker.AGENT
        assert msg.payload.customerId is None

    def test_system_with_business_identity_fails(self, valid_complete_msg: dict):
        valid_complete_msg["payload"]["agentId"] = "3210001"
        with pytest.raises(ValidationError, match="agentId must be null or omitted"):
            InboundMessage.model_validate(valid_complete_msg)

    def test_system_with_customer_id_fails(self, valid_complete_msg: dict):
        valid_complete_msg["payload"]["customerId"] = "12345678"
        with pytest.raises(ValidationError, match="customerId must be null or omitted"):
            InboundMessage.model_validate(valid_complete_msg)

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

    def test_removed_metadata_agent_id_field_is_rejected(self, valid_ongoing_msg: dict):
        valid_ongoing_msg["metaData"]["agentId"] = "3210001"
        with pytest.raises(ValidationError):
            InboundMessage.model_validate(valid_ongoing_msg)

    def test_removed_staff_id_field_is_rejected(self, valid_ongoing_msg: dict):
        valid_ongoing_msg["metaData"]["staffId"] = "45163407"
        with pytest.raises(ValidationError):
            InboundMessage.model_validate(valid_ongoing_msg)

    def test_extra_payload_field_is_rejected(self, valid_ongoing_msg: dict):
        valid_ongoing_msg["payload"]["staffId"] = "45163407"
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


class TestBuildSuccessAck:
    def test_transcript_ack_structure(self, conversation_id: str):
        ack = build_transcript_ack(conversation_id, 5)
        assert ack["metaData"]["conversationId"] == conversation_id
        assert ack["metaData"]["eventType"] == ResponseEventType.TRANSCRIPT_ACK.value
        assert ack["payload"]["sequenceNumber"] == 5
        assert "createdAtTimeStamp" in ack["payload"]

    def test_eol_ack_structure(self, conversation_id: str):
        ack = build_eol_ack(conversation_id, 42)
        assert ack["metaData"]["conversationId"] == conversation_id
        assert ack["metaData"]["eventType"] == ResponseEventType.EOL_ACK.value
        assert ack["payload"]["sequenceNumber"] == 42
        assert "createdAtTimeStamp" in ack["payload"]


class TestBuildError:
    def test_error_structure(self, conversation_id: str):
        err = build_error(conversation_id, "E1006", "Out of order", "seq=5 unexpected")
        assert err["metaData"]["eventType"] == ResponseEventType.ERROR.value
        assert err["error"]["code"] == "E1006"
        assert err["error"]["details"] == "seq=5 unexpected"

    def test_error_without_details(self, conversation_id: str):
        err = build_error(conversation_id, "E1007", "Internal error")
        assert err["error"]["details"] is None

