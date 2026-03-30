"""Shared test fixtures."""

from pathlib import Path
import sys

import pytest


_SRC_ROOT = Path(__file__).resolve().parents[1] / "src"
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))


@pytest.fixture
def conversation_id() -> str:
    return "39449992-32f3-4581-a8a1-99d4109f37d4"


@pytest.fixture
def valid_ongoing_msg(conversation_id: str) -> dict:
    """Valid SESSION_ONGOING message dict."""
    return {
        "metaData": {
            "conversationId": conversation_id,
            "callStartTimeStamp": "2025-03-21T10:30:02.327Z",
            "callEndTimeStamp": None,
            "eventType": "SESSION_ONGOING",
        },
        "payload": {
            "sequenceNumber": 0,
            "speaker": "Agent",
            "transcript": "thank you",
            "engineProvider": "FanoLabs",
            "dialect": "yue-x-auto",
            "isFinal": True,
            "speakTimeStamp": "2025-03-21T10:32:18.000Z",
            "transcriptGenerateTimeStamp": "2025-03-21T10:32:20.000Z",
        },
    }


@pytest.fixture
def valid_complete_msg(conversation_id: str) -> dict:
    """Valid SESSION_COMPLETE message dict."""
    return {
        "metaData": {
            "conversationId": conversation_id,
            "callStartTimeStamp": "2025-03-21T10:30:02.327Z",
            "callEndTimeStamp": "2025-03-21T10:45:00.000Z",
            "eventType": "SESSION_COMPLETE",
        },
        "payload": {
            "sequenceNumber": 42,
            "speaker": "System",
            "transcript": "session ended",
            "engineProvider": "FanoLabs",
            "dialect": "yue-x-auto",
            "isFinal": True,
        },
    }
