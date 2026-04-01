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
# Process / bootstrap keys (read before or outside pydantic-settings).
APP_ENV_VAR = "APP_ENV"
AWS_SECRETS_MANAGER_SECRET_ID_ENV = "AWS_SECRETS_MANAGER_SECRET_ID"
AWS_REGION_ENV = "AWS_REGION"
AWS_DEFAULT_REGION_ENV = "AWS_DEFAULT_REGION"
LOG_LEVEL_ENV = "LOG_LEVEL"
LOG_FORMAT_ENV = "LOG_FORMAT"

# Never overridden by Secret / .env merge — always taken from the process env at loader start.
DEPLOYED_BOOTSTRAP_ENV_KEYS = frozenset(
    {
        APP_ENV_VAR,
        AWS_SECRETS_MANAGER_SECRET_ID_ENV,
        AWS_REGION_ENV,
        AWS_DEFAULT_REGION_ENV,
    }
)

APP_ENV_LOCAL = "local"
APP_ENV_DEPLOYED = "deployed"
LOCAL_REDIS_URL = "redis://127.0.0.1:6379/0"
LOCAL_KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092"

# ---- Defaults (single source for settings + code defaults) ----
DEFAULT_KAFKA_TOPIC = "AI_STAGING_TRANSCRIPTION"

APP_ENV = Literal["local", "deployed"]
KAFKA_MODE = Literal["local", "aws_msk"]
COMPRESSION_TYPE = Literal["none", "gzip", "snappy", "lz4", "zstd"]
LOG_LEVEL = Literal["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"]
LOG_FORMAT = Literal["json", "console", "auto"]
