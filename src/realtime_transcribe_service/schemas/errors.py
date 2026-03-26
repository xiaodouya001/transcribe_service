"""应用错误码与 WebSocket Close Code 枚举 — 对齐 API Contract §4。"""

from __future__ import annotations

from enum import Enum


class ErrorCode(str, Enum):
    """应用错误码，对应 API Contract §4.3。"""

    E1001 = "E1001"  # JSON 解析失败
    E1002 = "E1002"  # 枚举值非法
    E1003 = "E1003"  # 缺少必填字段
    E1004 = "E1004"  # 字段类型不符
    E1005 = "E1005"  # 时间格式无效
    E1006 = "E1006"  # 序列号乱序
    E1007 = "E1007"  # 服务端内部异常
    E1008 = "E1008"  # 下游不可用
    E1009 = "E1009"  # 策略冲突
    E1010 = "E1010"  # 鉴权失败
    E1011 = "E1011"  # 下游超时


class WsCloseCode(int, Enum):
    """WebSocket Close Code，对应 API Contract §4.2。"""

    NORMAL = 1000
    GOING_AWAY = 1001
    INVALID_PAYLOAD = 1007  # JSON 解析/类型/格式错误
    POLICY_VIOLATION = 1008  # 业务规则、鉴权或策略违规
    INTERNAL_ERROR = 1011  # 服务端内部异常
    TRY_AGAIN_LATER = 1013  # 临时过载 / 下游不可用


def close_code_for_error(code: ErrorCode) -> WsCloseCode:
    """根据错误码返回对应的 WebSocket Close Code。"""
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
