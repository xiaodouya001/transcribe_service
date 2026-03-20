"""响应契约 — Server → Client 消息结构，严格对齐 API Contract §3。"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field

from transcribe_service.constants import (
    EVENT_ERROR,
    EVENT_TRANSCRIPT_ACK,
    MAX_ERROR_DETAILS_LEN,
    MAX_ERROR_MESSAGE_LEN,
)


# ---------------------------------------------------------------------------
# TRANSCRIPT_ACK
# ---------------------------------------------------------------------------

class AckMetaData(BaseModel):
    conversationId: str
    eventType: str = EVENT_TRANSCRIPT_ACK


class AckPayload(BaseModel):
    sequenceNumber: int
    createdAtTimeStamp: str
    serverProcessingMs: Optional[float] = None


class TranscriptAckResponse(BaseModel):
    metaData: AckMetaData
    payload: AckPayload


# ---------------------------------------------------------------------------
# ERROR
# ---------------------------------------------------------------------------

class ErrorMetaData(BaseModel):
    conversationId: str
    eventType: str = EVENT_ERROR


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

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def build_ack(conversation_id: str, sequence_number: int) -> dict:
    """构建 TRANSCRIPT_ACK 响应字典（可直接 JSON 序列化）。"""
    resp = TranscriptAckResponse(
        metaData=AckMetaData(conversationId=conversation_id),
        payload=AckPayload(
            sequenceNumber=sequence_number,
            createdAtTimeStamp=_utc_now_iso(),
        ),
    )
    return resp.model_dump()


def build_error(
    conversation_id: str,
    code: str,
    message: str,
    details: str | None = None,
) -> dict:
    """构建 ERROR 响应字典（可直接 JSON 序列化）。"""
    resp = ErrorResponse(
        metaData=ErrorMetaData(conversationId=conversation_id),
        error=ErrorDetail(
            code=code,
            message=message,
            details=details,
            createdAtTimeStamp=_utc_now_iso(),
        ),
    )
    return resp.model_dump()
