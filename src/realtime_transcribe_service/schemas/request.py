"""请求契约 — Client → Server 消息结构，严格对齐 API Contract §2。"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from pydantic_core import PydanticCustomError

from realtime_transcribe_service.schemas.events import EventType, Speaker


class MetaData(BaseModel):
    """请求元数据。"""

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
    """请求负载。"""

    model_config = ConfigDict(extra="forbid")

    agentId: Optional[str] = Field(None, max_length=32)
    customerId: Optional[str] = Field(None, max_length=64)
    sequenceNumber: int = Field(..., ge=0)
    speaker: Speaker
    transcript: str = Field(..., max_length=8000)
    engineProvider: str = Field(..., max_length=64)
    dialect: Optional[str] = Field(None, max_length=32)
    isFinal: bool
    createdAtTimeStamp: datetime

    @field_validator("agentId", "customerId")
    @classmethod
    def _ensure_non_blank_identifier(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if not value.strip():
            raise ValueError("identifier must not be empty")
        return value

    @field_validator("createdAtTimeStamp")
    @classmethod
    def _ensure_utc_timestamp(cls, value: datetime) -> datetime:
        if value.utcoffset() != timezone.utc.utcoffset(None):
            raise PydanticCustomError(
                "datetime_not_utc",
                "Input should be an ISO-8601 UTC timestamp",
            )
        return value


class InboundMessage(BaseModel):
    """完整上行消息 = metaData + payload，含跨字段业务规则校验。"""

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

        if evt == EventType.SESSION_COMPLETE:
            if not self.metaData.callEndTimeStamp:
                raise ValueError(
                    "callEndTimeStamp must be provided when eventType=SESSION_COMPLETE"
                )
            if speaker != Speaker.SYSTEM:
                raise ValueError("speaker must be System when eventType=SESSION_COMPLETE")

        if not self.payload.isFinal:
            raise ValueError("isFinal must be true")

        if speaker == Speaker.AGENT:
            if self.payload.agentId is None:
                raise ValueError("agentId must be provided when speaker=Agent")
            if self.payload.customerId is not None:
                raise ValueError("customerId must be null or omitted when speaker=Agent")
        elif speaker == Speaker.CUSTOMER:
            if self.payload.customerId is None:
                raise ValueError("customerId must be provided when speaker=Customer")
            if self.payload.agentId is not None:
                raise ValueError("agentId must be null or omitted when speaker=Customer")
        elif speaker == Speaker.SYSTEM:
            if self.payload.agentId is not None:
                raise ValueError("agentId must be null or omitted when speaker=System")
            if self.payload.customerId is not None:
                raise ValueError("customerId must be null or omitted when speaker=System")

        return self

