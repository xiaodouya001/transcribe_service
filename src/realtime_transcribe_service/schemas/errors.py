"""Application error codes and WebSocket close-code enums aligned with API Contract §4."""

from __future__ import annotations

from enum import Enum


class ErrorCode(str, Enum):
    """Application error codes as defined in API Contract §4.3."""

    E1001 = "E1001"  # JSON parsing failed
    E1002 = "E1002"  # Invalid enum value
    E1003 = "E1003"  # Missing required field
    E1004 = "E1004"  # Field type mismatch
    E1005 = "E1005"  # Invalid timestamp format
    E1006 = "E1006"  # Sequence number out of order
    E1007 = "E1007"  # Internal server exception
    E1008 = "E1008"  # Downstream unavailable
    E1009 = "E1009"  # Policy conflict
    E1010 = "E1010"  # Authentication failed
    E1011 = "E1011"  # Downstream timeout


class WsCloseCode(int, Enum):
    """WebSocket close codes as defined in API Contract §4.2."""

    NORMAL = 1000
    GOING_AWAY = 1001
    INVALID_PAYLOAD = 1007  # JSON parse or decode error
    POLICY_VIOLATION = 1008  # Business-rule, authentication, or policy violation
    INTERNAL_ERROR = 1011  # Internal server exception
    TRY_AGAIN_LATER = 1013  # Temporary overload or downstream issue


def close_code_for_error(code: ErrorCode) -> WsCloseCode:
    """Return the WebSocket close code mapped from the given application error code."""
    if code == ErrorCode.E1001:
        return WsCloseCode.INVALID_PAYLOAD
    if code in (
        ErrorCode.E1002,
        ErrorCode.E1003,
        ErrorCode.E1004,
        ErrorCode.E1005,
        ErrorCode.E1006,
    ):
        return WsCloseCode.POLICY_VIOLATION
    if code == ErrorCode.E1007:
        return WsCloseCode.INTERNAL_ERROR
    if code in (ErrorCode.E1008, ErrorCode.E1011):
        return WsCloseCode.TRY_AGAIN_LATER
    return WsCloseCode.POLICY_VIOLATION
