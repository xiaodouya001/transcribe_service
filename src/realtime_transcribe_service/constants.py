"""Global constants shared across modules: paths, field limits, and other immutables."""
from typing import Literal

# ---- Application metadata ----
APP_TITLE = "Realtime Transcribe Service"

# ---- WebSocket route ----
WS_PATH = "/ws/v1/realtime-transcriptions"

# ---- Field length limits (aligned with API Contract §3) ----
MAX_ERROR_MESSAGE_LEN = 256
MAX_ERROR_DETAILS_LEN = 2048

# ---- WebSocket close reasons ----
WS_CLOSE_REASON_GOING_AWAY = "Going away"

# ---- Environment variables ----
APP_ENV_LOCAL = "local"
APP_ENV_DEPLOYED = "deployed"
LOCAL_REDIS_URL = "redis://127.0.0.1:6379/0"
LOCAL_KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"

APP_ENV = Literal[APP_ENV_LOCAL, APP_ENV_DEPLOYED]
KAFKA_MODE = Literal["admin", "aws_msk"]
COMPRESSION_TYPE = Literal["none", "gzip", "snappy", "lz4", "zstd"]
KAFKA_SECURITY_PROTOCOL = Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"]
KAFKA_SASL_MECHANISM = Literal["SCRAM-SHA-256", "SCRAM-SHA-512"]
LOG_LEVEL = Literal["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"]
LOG_FORMAT = Literal["json", "console", "auto"]
