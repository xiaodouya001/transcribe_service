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
