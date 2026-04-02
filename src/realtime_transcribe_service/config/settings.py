"""Configuration loaded from environment variables."""

import os
from functools import lru_cache
from typing import Literal

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from realtime_transcribe_service.config.secrets_loader import merge_deployed_secrets_into_environ
from realtime_transcribe_service.constants import (
    APP_ENV,
    APP_ENV_DEPLOYED,
    APP_ENV_VAR,
    COMPRESSION_TYPE,
    DEFAULT_HTTP_BACKLOG,
    DEFAULT_HTTP_PORT,
    DEFAULT_KAFKA_BATCH_SIZE,
    DEFAULT_KAFKA_LINGER_MS,
    DEFAULT_KAFKA_SEND_TIMEOUT_SEC,
    DEFAULT_KAFKA_STARTUP_TIMEOUT_SEC,
    DEFAULT_KAFKA_TOPIC,
    DEFAULT_LOG_SLOW_MESSAGE_THRESHOLD_MS,
    DEFAULT_REDIS_ACTIVE_TTL_SEC,
    DEFAULT_REDIS_FINAL_TTL_SEC,
    DEFAULT_REDIS_MAX_CONNECTIONS,
    DEFAULT_REDIS_OWNERSHIP_GUARD_KEY_PREFIX,
    DEFAULT_REDIS_OWNERSHIP_GUARD_TTL_SEC,
    DEFAULT_REDIS_SEQUENCE_STATE_KEY_PREFIX,
    DEFAULT_REDIS_SSL_CHECK_HOSTNAME,
    DEFAULT_STOP_TIMEOUT,
    DEFAULT_WS_OWNERSHIP_GUARD_REFRESH_INTERVAL_SEC,
    DEFAULT_WS_PING_INTERVAL,
    DEFAULT_WS_PING_TIMEOUT,
    LOG_LEVEL,
    LOG_FORMAT,
    KAFKA_MODE,
    APP_ENV_LOCAL,
    LOCAL_REDIS_URL,
    LOCAL_KAFKA_BOOTSTRAP_SERVERS,
)


def normalize_url_path_prefix_str(raw: str) -> str:
    """Normalize ``URL_PATH_PREFIX``: empty means disabled; otherwise ``/abc`` (no trailing slash)."""
    if not isinstance(raw, str):
        return ""
    s = raw.strip()
    if not s:
        return ""
    if "\n" in s or "\r" in s or ".." in s:
        raise ValueError(
            "URL_PATH_PREFIX must not contain '..' or newline characters"
        )
    if not s.startswith("/"):
        s = "/" + s
    while "//" in s:
        s = s.replace("//", "/")
    s = s.rstrip("/")
    if not s:
        return ""
    return s


class Settings(BaseSettings):
    """Application settings from environment.

    **Local** (``APP_ENV=local``): pydantic-settings default precedence — **process environment
    overrides ``.env``** for the same variable name.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="forbid",
        validate_default=True,
    )

    app_env: APP_ENV

    # --- Redis ---
    redis_url: str | None = None
    redis_username: str | None = None
    redis_password: str | None = None
    redis_ssl_check_hostname: bool = DEFAULT_REDIS_SSL_CHECK_HOSTNAME
    redis_max_connections: int = Field(default=DEFAULT_REDIS_MAX_CONNECTIONS, gt=0)
    redis_active_ttl_sec: int = Field(default=DEFAULT_REDIS_ACTIVE_TTL_SEC, gt=0)
    redis_final_ttl_sec: int = Field(default=DEFAULT_REDIS_FINAL_TTL_SEC, gt=0)
    redis_ownership_guard_ttl_sec: int = Field(
        default=DEFAULT_REDIS_OWNERSHIP_GUARD_TTL_SEC, gt=0
    )
    redis_sequence_state_key_prefix: str = Field(
        default=DEFAULT_REDIS_SEQUENCE_STATE_KEY_PREFIX,
        min_length=1,
    )
    redis_ownership_guard_key_prefix: str = Field(
        default=DEFAULT_REDIS_OWNERSHIP_GUARD_KEY_PREFIX,
        min_length=1,
    )

    # --- Kafka ---
    kafka_bootstrap_servers: str | None = None
    kafka_mode: KAFKA_MODE = "local"
    kafka_topic: str = Field(default=DEFAULT_KAFKA_TOPIC, min_length=1)
    kafka_compression_type: COMPRESSION_TYPE = "zstd"
    kafka_ssl_ca_file: str | None = None
    kafka_aws_region: str | None = None
    kafka_aws_debug_creds: bool = False
    kafka_send_timeout_sec: float = Field(default=DEFAULT_KAFKA_SEND_TIMEOUT_SEC, gt=0)
    kafka_linger_ms: int = Field(default=DEFAULT_KAFKA_LINGER_MS, ge=0)
    kafka_batch_size: int = Field(default=DEFAULT_KAFKA_BATCH_SIZE, gt=0)

    # --- WebSocket ---
    # Passed to Uvicorn `ws="websockets"`: the server sends RFC Ping frames on an interval
    # and relies on peer Pong frames in response (see main.py).
    ws_ping_interval: float = Field(default=DEFAULT_WS_PING_INTERVAL, gt=0)
    ws_ping_timeout: float = Field(default=DEFAULT_WS_PING_TIMEOUT, gt=0)
    # Background ownership-guard refresh interval in seconds, used only while the connection is alive.
    ws_ownership_guard_refresh_interval_sec: float = Field(
        default=DEFAULT_WS_OWNERSHIP_GUARD_REFRESH_INTERVAL_SEC, gt=0
    )

    # --- Handshake authentication ---
    auth_enabled: bool = False
    auth_jwt_signing_material: str | None = None
    auth_jwt_algorithm: Literal["HS256"] = "HS256"

    # --- HTTP / Uvicorn ---
    http_port: int = Field(default=DEFAULT_HTTP_PORT, ge=1, le=65535)
    # listen() backlog. If set too low, bursty connection spikes can cause peers to get reset
    # before they read the 101 Switching Protocols response.
    http_backlog: int = Field(default=DEFAULT_HTTP_BACKLOG, gt=0)
    # Only an explicit true exposes /docs, /redoc, and /openapi.json.
    http_enable_docs: bool = False
    # When non-empty, the ASGI app is mounted so every route is under this path prefix
    # (for example ALB path /abc -> set /abc or abc; clients call /abc/health, /abc/ws/v1/...).
    url_path_prefix: str = Field(default="", max_length=512)
    # Maximum concurrent WebSocket connections. New handshakes beyond the limit are rejected.
    # 0 means unlimited.
    ws_max_connections: int = Field(default=0, ge=0)

    # --- Startup ---
    kafka_startup_timeout_sec: float = Field(default=DEFAULT_KAFKA_STARTUP_TIMEOUT_SEC, gt=0)

    # --- Graceful shutdown ---
    stop_timeout: float = Field(default=DEFAULT_STOP_TIMEOUT, gt=0)

    # --- Logging ---
    log_level: LOG_LEVEL = "INFO"
    log_format: LOG_FORMAT = "auto"
    # Whether to log the full JSON body of outgoing ERROR responses. Disabled by default to
    # avoid oversized load-test logs.
    log_ws_error_frames: bool = False
    # Threshold in milliseconds for slow-message stage timing warnings. 0 disables it.
    log_slow_message_threshold_ms: float = Field(
        default=DEFAULT_LOG_SLOW_MESSAGE_THRESHOLD_MS, ge=0
    )

    @field_validator("redis_username", mode="before")
    @classmethod
    def _optional_redis_username(cls, value: object) -> object:
        if value is None:
            return None
        if isinstance(value, str):
            stripped = value.strip()
            return stripped if stripped else None
        return value

    @field_validator("redis_password", mode="before")
    @classmethod
    def _optional_redis_password(cls, value: object) -> object:
        if value is None:
            return None
        if isinstance(value, str) and not value.strip():
            return None
        return value

    @field_validator("app_env", mode="before")
    @classmethod
    def _normalize_app_env(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip().lower()
        return value

    @field_validator("log_level", mode="before")
    @classmethod
    def _normalize_uppercase_enums(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip().upper()
        return value

    @field_validator("auth_jwt_algorithm", mode="before")
    @classmethod
    def _normalize_auth_jwt_algorithm(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip().upper()
        return value

    @field_validator("log_format", "kafka_compression_type", "kafka_mode", mode="before")
    @classmethod
    def _normalize_lowercase_enums(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip().lower()
        return value

    @field_validator("url_path_prefix", mode="after")
    @classmethod
    def _normalize_url_path_prefix(cls, value: object) -> object:
        if not isinstance(value, str):
            return ""
        return normalize_url_path_prefix_str(value)

    @field_validator(
        "redis_url",
        "kafka_bootstrap_servers",
        "kafka_topic",
        "kafka_ssl_ca_file",
        "kafka_aws_region",
        "redis_sequence_state_key_prefix",
        "redis_ownership_guard_key_prefix",
        "auth_jwt_signing_material",
        mode="before",
    )
    @classmethod
    def _reject_blank_strings(cls, value: object) -> object:
        if value is None:
            return value
        if isinstance(value, str):
            normalized = value.strip()
            if not normalized:
                raise ValueError("must not be empty")
            return normalized
        return value

    @model_validator(mode="after")
    def _apply_environment_rules(self) -> "Settings":
        if self.app_env == APP_ENV_LOCAL:
            if self.redis_url is None:
                self.redis_url = LOCAL_REDIS_URL
            if self.kafka_bootstrap_servers is None:
                self.kafka_bootstrap_servers = LOCAL_KAFKA_BOOTSTRAP_SERVERS
        else:
            missing: list[str] = []
            if self.redis_url is None:
                missing.append("REDIS_URL")
            if self.kafka_bootstrap_servers is None:
                missing.append("KAFKA_BOOTSTRAP_SERVERS")
            if missing:
                raise ValueError(
                    "Missing required configuration for APP_ENV=deployed: "
                    + ", ".join(missing)
                )
            # ElastiCache user ACLs usually scope keys by namespace; code defaults have no
            # account/env prefix. Require explicit prefixes on deployed so operators align ACLs.
            if self.redis_sequence_state_key_prefix == DEFAULT_REDIS_SEQUENCE_STATE_KEY_PREFIX:
                raise ValueError(
                    "APP_ENV=deployed requires explicit REDIS_SEQUENCE_STATE_KEY_PREFIX "
                    "matching your Redis/ElastiCache ACL key pattern (defaults are for "
                    "APP_ENV=local / unscoped Redis only)."
                )
            if self.redis_ownership_guard_key_prefix == DEFAULT_REDIS_OWNERSHIP_GUARD_KEY_PREFIX:
                raise ValueError(
                    "APP_ENV=deployed requires explicit REDIS_OWNERSHIP_GUARD_KEY_PREFIX "
                    "matching your Redis/ElastiCache ACL key pattern (defaults are for "
                    "APP_ENV=local / unscoped Redis only)."
                )

        if self.redis_final_ttl_sec > self.redis_active_ttl_sec:
            raise ValueError("REDIS_FINAL_TTL_SEC must be <= REDIS_ACTIVE_TTL_SEC")

        if self.ws_ownership_guard_refresh_interval_sec >= self.redis_ownership_guard_ttl_sec:
            raise ValueError(
                "WS_OWNERSHIP_GUARD_REFRESH_INTERVAL_SEC must be < "
                "REDIS_OWNERSHIP_GUARD_TTL_SEC"
            )

        if self.auth_enabled and self.auth_jwt_signing_material is None:
            raise ValueError(
                "AUTH_JWT_SIGNING_MATERIAL must be set when AUTH_ENABLED=true"
            )

        if self.app_env == APP_ENV_DEPLOYED and self.kafka_mode != "aws_msk":
            raise ValueError(
                "APP_ENV=deployed requires KAFKA_MODE=aws_msk "
                "(KAFKA_MODE=local is for local docker-compose only)"
            )

        if self.kafka_mode == "local" and self.kafka_ssl_ca_file is not None:
            raise ValueError(
                "KAFKA_SSL_CA_FILE is only used when KAFKA_MODE=aws_msk; "
                "local mode is PLAINTEXT docker-compose only"
            )

        if self.kafka_mode == "aws_msk":
            if self.kafka_aws_region is None:
                raise ValueError(
                    "KAFKA_AWS_REGION must be set when KAFKA_MODE=aws_msk"
                )

        return self


@lru_cache
def get_settings() -> Settings:
    """Cached settings instance.

    When ``APP_ENV=deployed``, :func:`~realtime_transcribe_service.config.secrets_loader.merge_deployed_secrets_into_environ`
    merges ``.env`` (cwd), process environment, and Secrets Manager JSON into :data:`os.environ`
    (see that module for precedence), then builds ``Settings`` with ``_env_file=None`` so ``.env``
    is not read twice.
    """
    merge_deployed_secrets_into_environ()
    if os.environ.get(APP_ENV_VAR, "").strip().lower() == APP_ENV_DEPLOYED:
        return Settings(_env_file=None)  # pyright: ignore[reportCallIssue]
    return Settings()  # pyright: ignore[reportCallIssue]
