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
SUPPRESS_HEALTH_ACCESS_LOGS_ENV = "SUPPRESS_HEALTH_ACCESS_LOGS"
URL_PATH_PREFIX_ENV = "URL_PATH_PREFIX"

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
DEFAULT_REDIS_MAX_CONNECTIONS = 100
DEFAULT_REDIS_ACTIVE_TTL_SEC = 3600
DEFAULT_REDIS_FINAL_TTL_SEC = 60
DEFAULT_REDIS_OWNERSHIP_GUARD_TTL_SEC = 30
# Service-scoped Redis key stems (no account/environment namespace). Used as defaults for
# APP_ENV=local. For APP_ENV=deployed, Settings rejects these defaults so you must set
# REDIS_SEQUENCE_STATE_KEY_PREFIX / REDIS_OWNERSHIP_GUARD_KEY_PREFIX to strings allowed by
# your ElastiCache user ACL (otherwise you may get NOPERM at runtime even if PING works).
DEFAULT_REDIS_SEQUENCE_STATE_KEY_PREFIX = (
    "realtime-transcribe-service:expect-transcript-seq-num"
)
DEFAULT_REDIS_OWNERSHIP_GUARD_KEY_PREFIX = (
    "realtime-transcribe-service:conversation-owner"
)
DEFAULT_REDIS_SSL_CHECK_HOSTNAME = False

DEFAULT_KAFKA_SEND_TIMEOUT_SEC = 2.0
DEFAULT_KAFKA_LINGER_MS = 1
DEFAULT_KAFKA_BATCH_SIZE = 32768

DEFAULT_WS_PING_INTERVAL = 20.0
DEFAULT_WS_PING_TIMEOUT = 10.0
DEFAULT_WS_OWNERSHIP_GUARD_REFRESH_INTERVAL_SEC = 15.0

DEFAULT_HTTP_HOST = "0.0.0.0"
DEFAULT_HTTP_PORT = 8080
DEFAULT_HTTP_BACKLOG = 4096

DEFAULT_KAFKA_STARTUP_TIMEOUT_SEC = 30.0
DEFAULT_STOP_TIMEOUT = 120.0
DEFAULT_LOG_SLOW_MESSAGE_THRESHOLD_MS = 0.0

APP_ENV = Literal["local", "deployed"]
KAFKA_MODE = Literal["local", "aws_msk"]
COMPRESSION_TYPE = Literal["none", "gzip", "snappy", "lz4", "zstd"]
LOG_LEVEL = Literal["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"]
LOG_FORMAT = Literal["json", "console", "auto"]
