"""Kafka outbound schema models."""

from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, field_validator
from pydantic_core import PydanticCustomError

from realtime_transcribe_service.schemas.request import MetaData, Payload


class KafkaEnrich(BaseModel):
    """Service-enriched Kafka metadata."""

    model_config = ConfigDict(extra="forbid")

    eventProduceTimestamp: datetime

    @field_validator("eventProduceTimestamp")
    @classmethod
    def _ensure_utc_timestamp(cls, value: datetime) -> datetime:
        if value.utcoffset() != timezone.utc.utcoffset(None):
            raise PydanticCustomError(
                "datetime_not_utc",
                "Input should be an ISO-8601 UTC timestamp",
            )
        return value


class KafkaOutboundMessage(BaseModel):
    """Kafka value payload written on success path."""

    model_config = ConfigDict(extra="forbid")

    metaData: MetaData
    payload: Payload
    enrich: KafkaEnrich
