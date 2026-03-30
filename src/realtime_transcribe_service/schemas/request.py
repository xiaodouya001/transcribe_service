"""Request contract — client-to-server message shape, aligned with API Contract §2."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic_core import PydanticCustomError

from realtime_transcribe_service.schemas.events import EventType, Speaker


class MetaData(BaseModel):
    """Request metadata."""

    model_config = ConfigDict(extra="forbid")

    conversationId: str = Field(..., max_length=64)
    callStartTimeStamp: datetime
    callEndTimeStamp: Optional[datetime] = None
    eventType: EventType

    @field_validator("callStartTimeStamp", "callEndTimeStamp")
    @classmethod
    def _ensure_utc_timestamp(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.utcoffset() != timezone.utc.utcoffset(None):
            raise PydanticCustomError(
                "datetime_not_utc",
                "Input should be an ISO-8601 UTC timestamp",
            )
        return value


class Payload(BaseModel):
    """Request payload."""

    model_config = ConfigDict(extra="forbid")

    sequenceNumber: int = Field(..., ge=0)
    speaker: Speaker
    transcript: str = Field(..., max_length=8000)
    engineProvider: str = Field(..., max_length=64)
    dialect: str = Field(..., max_length=32)
    isFinal: bool
    speakTimeStamp: Optional[datetime] = None
    transcriptGenerateTimeStamp: Optional[datetime] = None

    @field_validator("speakTimeStamp", "transcriptGenerateTimeStamp")
    @classmethod
    def _ensure_utc_timestamp(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.utcoffset() != timezone.utc.utcoffset(None):
            raise PydanticCustomError(
                "datetime_not_utc",
                "Input should be an ISO-8601 UTC timestamp",
            )
        return value


class InboundMessage(BaseModel):
    """Complete inbound message = metaData + payload, including cross-field business rules."""

    model_config = ConfigDict(extra="forbid")

    metaData: MetaData
    payload: Payload

    @model_validator(mode="after")
    def _check_business_rules(self) -> "InboundMessage":
        evt = self.metaData.eventType
        speaker = self.payload.speaker

        if evt == EventType.SESSION_ONGOING:
            if self.metaData.callEndTimeStamp is not None:
                raise ValueError(
                    "callEndTimeStamp must be null when eventType=SESSION_ONGOING"
                )
            if speaker not in {Speaker.AGENT, Speaker.CUSTOMER}:
                raise ValueError(
                    "speaker must be Agent or Customer when eventType=SESSION_ONGOING"
                )
            if self.payload.speakTimeStamp is None:
                raise ValueError(
                    "speakTimeStamp must be provided when eventType=SESSION_ONGOING"
                )
            if self.payload.transcriptGenerateTimeStamp is None:
                raise ValueError(
                    "transcriptGenerateTimeStamp must be provided when eventType=SESSION_ONGOING"
                )

        if evt == EventType.SESSION_COMPLETE:
            if not self.metaData.callEndTimeStamp:
                raise ValueError(
                    "callEndTimeStamp must be provided when eventType=SESSION_COMPLETE"
                )
            if speaker != Speaker.SYSTEM:
                raise ValueError("speaker must be System when eventType=SESSION_COMPLETE")
            if "speakTimeStamp" in self.payload.model_fields_set:
                raise ValueError(
                    "speakTimeStamp must be omitted when eventType=SESSION_COMPLETE"
                )
            if "transcriptGenerateTimeStamp" in self.payload.model_fields_set:
                raise ValueError(
                    "transcriptGenerateTimeStamp must be omitted when eventType=SESSION_COMPLETE"
                )

        if not self.payload.isFinal:
            raise ValueError("isFinal must be true")

        return self

