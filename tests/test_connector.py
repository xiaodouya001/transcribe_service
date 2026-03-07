"""Tests for connector layer."""

import pytest
from asr_ingest.connector.base import TranscriptionEvent


def test_from_vendor_payload() -> None:
    """Parse Vendor payload into TranscriptionEvents."""
    payload = {
        "success": True,
        "result": {
            "processingId": "proc-123",
            "processingStatus": "IN_PROGRESS",
            "callStatus": {"sessionId": "s1"},
            "transcripts": [
                {"seqNo": 0, "transcript": "hello", "role": "Agent", "createdAt": "2025-01-01T00:00:00Z"},
                {"seqNo": 1, "transcript": "hi", "role": "Customer", "createdAt": "2025-01-01T00:00:01Z"},
            ],
        },
    }
    events = TranscriptionEvent.from_vendor_payload(payload)
    assert len(events) == 2
    assert events[0].session_id == "s1"
    assert events[0].processing_id == "proc-123"
    assert events[0].seq_no == 0
    assert events[0].transcript == "hello"
    assert events[0].role == "Agent"
    assert events[1].seq_no == 1
    assert events[1].transcript == "hi"


def test_from_vendor_payload_empty() -> None:
    """Empty or missing result yields empty list."""
    assert TranscriptionEvent.from_vendor_payload({}) == []
    assert TranscriptionEvent.from_vendor_payload({"result": {}}) == []
    assert TranscriptionEvent.from_vendor_payload({"result": {"transcripts": []}}) == []


def test_from_vendor_payload_missing_optional_fields() -> None:
    """Missing optional fields use defaults."""
    payload = {
        "result": {
            "callStatus": {"sessionId": "s1"},
            "transcripts": [
                {"seqNo": 0, "transcript": "hi"},
                {"seqNo": 1},
            ],
        },
    }
    events = TranscriptionEvent.from_vendor_payload(payload)
    assert len(events) == 2
    assert events[0].session_id == "s1"
    assert events[0].seq_no == 0
    assert events[0].transcript == "hi"
    assert events[0].role == ""
    assert events[1].seq_no == 1
    assert events[1].transcript == ""


def test_from_vendor_payload_camel_case_fields() -> None:
    """Vendor payload uses camelCase (seqNo, sessionId, etc)."""
    payload = {
        "success": True,
        "result": {
            "processingId": "proc-1",
            "processingStatus": "DONE",
            "callStatus": {"sessionId": "sid-123"},
            "transcripts": [
                {
                    "seqNo": 5,
                    "transcript": "test",
                    "role": "Customer",
                    "createdAt": "2025-01-01T12:00:00Z",
                },
            ],
        },
    }
    events = TranscriptionEvent.from_vendor_payload(payload)
    assert len(events) == 1
    assert events[0].processing_id == "proc-1"
    assert events[0].processing_status == "DONE"
    assert events[0].session_id == "sid-123"
    assert events[0].seq_no == 5
    assert events[0].created_at == "2025-01-01T12:00:00Z"
