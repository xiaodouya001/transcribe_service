"""Pytest fixtures."""

import pytest


@pytest.fixture
def sample_transcript_event():
    """Sample TranscriptionEvent-like dict for tests."""
    return {
        "session_id": "39449992-32f3-4581-a8a1-99d4109f37d4",
        "seq_no": 0,
        "transcript": "喂您好這裡是有光科技",
        "role": "Agent",
        "created_at": "2025-03-21T10:32:20.000Z",
        "processing_status": "IN_PROGRESS",
    }
