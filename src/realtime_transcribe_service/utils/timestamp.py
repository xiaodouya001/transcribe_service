"""Shared UTC timestamp helpers."""

from __future__ import annotations

from datetime import datetime, timezone


def format_utc_timestamp(value: datetime) -> str:
    """Format datetime as canonical UTC millisecond timestamp."""
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def utc_now_timestamp() -> str:
    """Return current UTC time in canonical timestamp format."""
    return format_utc_timestamp(datetime.now(timezone.utc))
