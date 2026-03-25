"""Schemas 契约层 — Pydantic 强类型数据网关，纯 CPU 内存级校验，禁止任何 I/O。"""

from transcribe_service.schemas.errors import ErrorCode, WsCloseCode, close_code_for_error
from transcribe_service.schemas.request import (
    EventType,
    InboundMessage,
    MetaData,
    Payload,
    Speaker,
)
from transcribe_service.schemas.response import (
    EolAckResponse,
    ErrorResponse,
    TranscriptAckResponse,
    build_eol_ack,
    build_error,
    build_transcript_ack,
)

__all__ = [
    "ErrorCode",
    "WsCloseCode",
    "close_code_for_error",
    "EventType",
    "InboundMessage",
    "MetaData",
    "Payload",
    "Speaker",
    "EolAckResponse",
    "ErrorResponse",
    "TranscriptAckResponse",
    "build_eol_ack",
    "build_error",
    "build_transcript_ack",
]
