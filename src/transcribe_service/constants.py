"""全局常量 — 路径、事件类型、字段长度等跨模块共享的不可变值。"""

# ---- 应用元信息 ----
APP_TITLE = "Transcribe Service"

# ---- WebSocket 路由 ----
WS_PATH = "/ws/v1/realtime-transcriptions"

# ---- 服务端响应事件类型 (Server → Client) ----
EVENT_TRANSCRIPT_ACK = "TRANSCRIPT_ACK"
EVENT_ERROR = "ERROR"

# ---- 字段长度上限（对齐 API Contract §3） ----
MAX_ERROR_MESSAGE_LEN = 256
MAX_ERROR_DETAILS_LEN = 2048

# ---- WebSocket 关闭理由 ----
WS_CLOSE_REASON_GOING_AWAY = "Going away"
