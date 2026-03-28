"""Response contract — server-to-client message shape, aligned with API Contract §3."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from realtime_transcribe_service.constants import (
    MAX_ERROR_DETAILS_LEN,
    MAX_ERROR_MESSAGE_LEN,
)
from realtime_transcribe_service.schemas.events import ResponseEventType
from realtime_transcribe_service.utils.timestamp import utc_now_timestamp


# ---------------------------------------------------------------------------
# TRANSCRIPT_ACK
# ---------------------------------------------------------------------------

class AckMetaData(BaseModel):
    conversationId: str
    eventType: ResponseEventType


class AckPayload(BaseModel):
    sequenceNumber: int
    createdAtTimeStamp: str
    serverProcessingMs: Optional[float] = None


class TranscriptAckResponse(BaseModel):
    metaData: AckMetaData
    payload: AckPayload


class EolAckResponse(BaseModel):
    metaData: AckMetaData
    payload: AckPayload


# ---------------------------------------------------------------------------
# ERROR
# ---------------------------------------------------------------------------

class ErrorMetaData(BaseModel):
    conversationId: str
    eventType: ResponseEventType = ResponseEventType.ERROR


class ErrorDetail(BaseModel):
    code: str = Field(..., max_length=16)
    message: str = Field(..., max_length=MAX_ERROR_MESSAGE_LEN)
    details: Optional[str] = Field(None, max_length=MAX_ERROR_DETAILS_LEN)
    createdAtTimeStamp: str


class ErrorResponse(BaseModel):
    metaData: ErrorMetaData
    error: ErrorDetail


# ---------------------------------------------------------------------------
# Builder helpers
# ---------------------------------------------------------------------------

def _build_success_response(
    conversation_id: str,
    sequence_number: int,
    event_type: ResponseEventType,
) -> dict:
    return {
        "metaData": {
            "conversationId": conversation_id,
            "eventType": event_type.value,
        },
        "payload": {
            "sequenceNumber": sequence_number,
            "createdAtTimeStamp": utc_now_timestamp(),
        },
    }


def build_transcript_ack(conversation_id: str, sequence_number: int) -> dict:
    """Build a ``TRANSCRIPT_ACK`` response dictionary ready for JSON serialization."""
    return _build_success_response(
        conversation_id, sequence_number, ResponseEventType.TRANSCRIPT_ACK
    )


def build_eol_ack(conversation_id: str, sequence_number: int) -> dict:
    """Build an ``EOL_ACK`` response dictionary ready for JSON serialization."""
    return _build_success_response(conversation_id, sequence_number, ResponseEventType.EOL_ACK)


def build_error(
    conversation_id: str,
    code: str,
    message: str,
    details: str | None = None,
) -> dict:
    """Build an ``ERROR`` response dictionary ready for JSON serialization."""
    return {
        "metaData": {
            "conversationId": conversation_id,
            "eventType": ResponseEventType.ERROR.value,
        },
        "error": {
            "code": code,
            "message": message,
            "details": details,
            "createdAtTimeStamp": utc_now_timestamp(),
        },
    }

