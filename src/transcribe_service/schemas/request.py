"""请求契约 — Client → Server 消息结构，严格对齐 API Contract §2。"""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_core import PydanticCustomError


class EventType(str, Enum):
    """上行事件类型。"""

    SESSION_ONGOING = "SESSION_ONGOING"
    SESSION_COMPLETE = "SESSION_COMPLETE"


class Speaker(str, Enum):
    """说话人角色。"""

    AGENT = "Agent"
    CUSTOMER = "Customer"


class MetaData(BaseModel):
    """请求元数据。"""

    conversationId: str = Field(..., max_length=64)
    agentId: str = Field(..., max_length=32)
    staffId: str = Field(..., max_length=32)
    customerId: str = Field(..., max_length=64)
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

    sequenceNumber: int = Field(..., ge=0)
    speaker: Speaker
    transcript: str = Field(..., max_length=8000)
    engineProvider: str = Field(..., max_length=64)
    dialect: Optional[str] = Field(None, max_length=32)
    isFinal: bool
    createdAtTimeStamp: datetime

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

    metaData: MetaData
    payload: Payload

    @model_validator(mode="after")
    def _check_business_rules(self) -> "InboundMessage":
        evt = self.metaData.eventType

        if evt == EventType.SESSION_ONGOING:
            if self.metaData.callEndTimeStamp is not None:
                raise ValueError(
                    "callEndTimeStamp must be null when eventType=SESSION_ONGOING"
                )

        if evt == EventType.SESSION_COMPLETE:
            if not self.metaData.callEndTimeStamp:
                raise ValueError(
                    "callEndTimeStamp must be provided when eventType=SESSION_COMPLETE"
                )

        if not self.payload.isFinal:
            raise ValueError("isFinal must be true")

        return self
