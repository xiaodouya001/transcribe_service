"""Tests for transform layer."""

import pytest
from asr_ingest.connector.base import TranscriptionEvent
from asr_ingest.transform import DefaultCleaner, IdentityCleaner, get_cleaner


def test_default_cleaner() -> None:
    """DefaultCleaner returns raw + cleaned."""
    raw = {"result": {"callStatus": {"sessionId": "s1"}, "transcripts": [{"seqNo": 0}]}}
    event = TranscriptionEvent("s1", 0, "hello", "Agent", processing_id="p1")
    cleaner = DefaultCleaner()
    out = cleaner.clean(raw, event)
    assert "raw" in out
    assert out["raw"] == raw
    assert "cleaned" in out
    c = out["cleaned"]
    assert c["session_id"] == "s1"
    assert c["seq_no"] == 0
    assert c["transcript"] == "hello"
    assert c["processing_id"] == "p1"


def test_identity_cleaner() -> None:
    """IdentityCleaner returns raw only."""
    raw = {"foo": "bar"}
    event = TranscriptionEvent("s1", 0, "x")
    cleaner = IdentityCleaner()
    out = cleaner.clean(raw, event)
    assert out == {"raw": raw}


def test_get_cleaner() -> None:
    """get_cleaner returns correct backend."""
    assert isinstance(get_cleaner("default"), DefaultCleaner)
    assert isinstance(get_cleaner("identity"), IdentityCleaner)
