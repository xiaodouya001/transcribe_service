"""Global constants shared across modules: paths, field limits, and other immutables."""

# ---- Application metadata ----
APP_TITLE = "Realtime Transcribe Service"

# ---- WebSocket route ----
WS_PATH = "/ws/v1/realtime-transcriptions"

# ---- Field length limits (aligned with API Contract §3) ----
MAX_ERROR_MESSAGE_LEN = 256
MAX_ERROR_DETAILS_LEN = 2048

# ---- WebSocket close reasons ----
WS_CLOSE_REASON_GOING_AWAY = "Going away"
