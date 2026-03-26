"""协议枚举 — 请求事件、响应事件与说话人角色。"""

from enum import Enum


class EventType(str, Enum):
    """上行事件类型。"""

    SESSION_ONGOING = "SESSION_ONGOING"
    SESSION_COMPLETE = "SESSION_COMPLETE"


class ResponseEventType(str, Enum):
    """下行事件类型。"""

    TRANSCRIPT_ACK = "TRANSCRIPT_ACK"
    EOL_ACK = "EOL_ACK"
    ERROR = "ERROR"


class Speaker(str, Enum):
    """说话人角色。"""

    AGENT = "Agent"
    CUSTOMER = "Customer"
    SYSTEM = "System"
