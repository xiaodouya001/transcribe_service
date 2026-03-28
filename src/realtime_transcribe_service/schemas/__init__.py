"""Schema contract layer — strongly typed Pydantic validation with no I/O."""

from realtime_transcribe_service.schemas.errors import ErrorCode, WsCloseCode, close_code_for_error
from realtime_transcribe_service.schemas.events import EventType, ResponseEventType, Speaker
from realtime_transcribe_service.schemas.request import (
    InboundMessage,
    MetaData,
    Payload,
)
from realtime_transcribe_service.schemas.kafka_outbound import KafkaEnrich, KafkaOutboundMessage
from realtime_transcribe_service.schemas.response import (
    EolAckResponse,
    ErrorResponse,
    TranscriptAckResponse,
    build_eol_ack,
    build_error,
    build_transcript_ack,
)
from realtime_transcribe_service.utils.timestamp import format_utc_timestamp, utc_now_timestamp

__all__ = [
    "ErrorCode",
    "WsCloseCode",
    "close_code_for_error",
    "EventType",
    "ResponseEventType",
    "InboundMessage",
    "MetaData",
    "Payload",
    "KafkaEnrich",
    "KafkaOutboundMessage",
    "Speaker",
    "EolAckResponse",
    "ErrorResponse",
    "TranscriptAckResponse",
    "build_eol_ack",
    "build_error",
    "build_transcript_ack",
    "format_utc_timestamp",
    "utc_now_timestamp",
]

