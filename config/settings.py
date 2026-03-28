"""Configuration loaded from environment variables."""

from functools import lru_cache
from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings from environment."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        env_ignore_empty=True,
    )

    # --- Redis ---
    redis_url: str = "redis://127.0.0.1:6379/0"
    redis_max_connections: int = 100
    redis_active_ttl_sec: int = 3600
    redis_final_ttl_sec: int = 60
    redis_ownership_guard_ttl_sec: int = 30
    redis_sequence_state_key_prefix: str = "realtime-transcribe-service:expect-transcript-seq-num"
    redis_ownership_guard_key_prefix: str = "realtime-transcribe-service:conversation-owner"

    # --- Kafka ---
    kafka_bootstrap_servers: str = "127.0.0.1:9092"
    kafka_topic: str = "AI_STAGING_TRANSCRIPTION"
    kafka_topic_num_partitions: int = 50
    kafka_replication_factor: int = 1
    kafka_compression_type: Literal["none", "gzip", "snappy", "lz4", "zstd"] = "zstd"
    kafka_send_timeout_sec: float = 2.0
    kafka_linger_ms: int = 1
    kafka_batch_size: int = 32768

    # --- WebSocket ---
    # Passed to Uvicorn `ws="websockets"`: the server sends RFC Ping frames on an interval
    # and relies on peer Pong frames in response (see main.py).
    ws_ping_interval: float = 20.0
    ws_ping_timeout: float = 10.0
    # Background ownership-guard refresh interval in seconds, used only while the connection is alive.
    ws_ownership_guard_refresh_interval_sec: float = 5.0

    # --- HTTP / Uvicorn ---
    http_host: str = "0.0.0.0"
    http_port: int = 8080
    # listen() backlog. If set too low, bursty connection spikes can cause peers to get reset
    # before they read the 101 Switching Protocols response.
    http_backlog: int = 4096
    # Maximum concurrent WebSocket connections. New handshakes beyond the limit are rejected.
    # 0 means unlimited.
    ws_max_connections: int = 0

    # --- Startup ---
    kafka_startup_timeout_sec: float = 30.0

    # --- Graceful shutdown ---
    stop_timeout: int = 120

    # --- Logging ---
    log_level: str = "INFO"
    log_format: Literal["json", "console", "auto"] = "auto"
    # Whether to log the full JSON body of outgoing ERROR responses. Disabled by default to
    # avoid oversized load-test logs.
    log_ws_error_frames: bool = False
    # Threshold in milliseconds for slow-message stage timing warnings. 0 disables it.
    log_slow_message_threshold_ms: float = 0.0


@lru_cache
def get_settings() -> Settings:
    """Cached settings instance."""
    return Settings()
