"""Protocol enums for request events, response events, and speaker roles."""

from enum import Enum


class EventType(str, Enum):
    """Inbound event types."""

    SESSION_ONGOING = "SESSION_ONGOING"
    SESSION_COMPLETE = "SESSION_COMPLETE"


class ResponseEventType(str, Enum):
    """Outbound event types."""

    TRANSCRIPT_ACK = "TRANSCRIPT_ACK"
    EOL_ACK = "EOL_ACK"
    ERROR = "ERROR"


class Speaker(str, Enum):
    """Speaker roles."""

    AGENT = "Agent"
    CUSTOMER = "Customer"
    SYSTEM = "System"
