"""Configuration loaded from environment variables."""

from functools import lru_cache
from typing import Literal

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from realtime_transcribe_service.constants import (
    APP_ENV,
    COMPRESSION_TYPE,
    LOG_LEVEL,
    LOG_FORMAT,
    APP_ENV_LOCAL,
    LOCAL_REDIS_URL,
    LOCAL_KAFKA_BOOTSTRAP_SERVERS,
)


class Settings(BaseSettings):
    """Application settings from environment."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="forbid",
        validate_default=True,
    )

    app_env: APP_ENV

    # --- Redis ---
    redis_url: str | None = None
    redis_max_connections: int = Field(default=100, gt=0)
    redis_active_ttl_sec: int = Field(default=3600, gt=0)
    redis_final_ttl_sec: int = Field(default=60, gt=0)
    redis_ownership_guard_ttl_sec: int = Field(default=30, gt=0)
    redis_sequence_state_key_prefix: str = Field(
        default="realtime-transcribe-service:expect-transcript-seq-num",
        min_length=1,
    )
    redis_ownership_guard_key_prefix: str = Field(
        default="realtime-transcribe-service:conversation-owner",
        min_length=1,
    )

    # --- Kafka ---
    kafka_bootstrap_servers: str | None = None
    kafka_topic: str = Field(default="AI_STAGING_TRANSCRIPTION", min_length=1)
    kafka_topic_num_partitions: int = Field(default=50, gt=0)
    kafka_replication_factor: int = Field(default=1, gt=0)
    kafka_compression_type: COMPRESSION_TYPE = "zstd"
    kafka_send_timeout_sec: float = Field(default=2.0, gt=0)
    kafka_linger_ms: int = Field(default=1, ge=0)
    kafka_batch_size: int = Field(default=32768, gt=0)

    # --- WebSocket ---
    # Passed to Uvicorn `ws="websockets"`: the server sends RFC Ping frames on an interval
    # and relies on peer Pong frames in response (see main.py).
    ws_ping_interval: float = Field(default=20.0, gt=0)
    ws_ping_timeout: float = Field(default=10.0, gt=0)
    # Background ownership-guard refresh interval in seconds, used only while the connection is alive.
    ws_ownership_guard_refresh_interval_sec: float = Field(default=5.0, gt=0)

    # --- HTTP / Uvicorn ---
    http_host: str = Field(default="0.0.0.0", min_length=1)
    http_port: int = Field(default=8080, ge=1, le=65535)
    # listen() backlog. If set too low, bursty connection spikes can cause peers to get reset
    # before they read the 101 Switching Protocols response.
    http_backlog: int = Field(default=4096, gt=0)
    # Maximum concurrent WebSocket connections. New handshakes beyond the limit are rejected.
    # 0 means unlimited.
    ws_max_connections: int = Field(default=0, ge=0)

    # --- Startup ---
    kafka_startup_timeout_sec: float = Field(default=30.0, gt=0)

    # --- Graceful shutdown ---
    stop_timeout: float = Field(default=120.0, gt=0)

    # --- Logging ---
    log_level: LOG_LEVEL = "INFO"
    log_format: LOG_FORMAT = "auto"
    # Whether to log the full JSON body of outgoing ERROR responses. Disabled by default to
    # avoid oversized load-test logs.
    log_ws_error_frames: bool = False
    # Threshold in milliseconds for slow-message stage timing warnings. 0 disables it.
    log_slow_message_threshold_ms: float = Field(default=0.0, ge=0)

    @field_validator("app_env", mode="before")
    @classmethod
    def _normalize_app_env(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip().lower()
        return value

    @field_validator("log_level", mode="before")
    @classmethod
    def _normalize_log_level(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip().upper()
        return value

    @field_validator("log_format", "kafka_compression_type", mode="before")
    @classmethod
    def _normalize_lowercase_enums(cls, value: object) -> object:
        if isinstance(value, str):
            return value.strip().lower()
        return value

    @field_validator(
        "redis_url",
        "kafka_bootstrap_servers",
        "kafka_topic",
        "redis_sequence_state_key_prefix",
        "redis_ownership_guard_key_prefix",
        "http_host",
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

        if self.redis_final_ttl_sec > self.redis_active_ttl_sec:
            raise ValueError("REDIS_FINAL_TTL_SEC must be <= REDIS_ACTIVE_TTL_SEC")

        if self.ws_ownership_guard_refresh_interval_sec >= self.redis_ownership_guard_ttl_sec:
            raise ValueError(
                "WS_OWNERSHIP_GUARD_REFRESH_INTERVAL_SEC must be < "
                "REDIS_OWNERSHIP_GUARD_TTL_SEC"
            )

        return self


@lru_cache
def get_settings() -> Settings:
    """Cached settings instance."""
    return Settings()
