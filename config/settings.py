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

    # Transcribe Service 直连模式（统一 Webhook，无模式分支）
    transcribe_service_max_sessions_per_pod: int = 100
    transcribe_service_protocol: str = "sse"  # "sse" 或 "websocket"
    transcribe_service_ssrf_allow_localhost: bool = False  # True 时允许 127.0.0.1（仅 Demo）

    # Redis
    redis_url: str = "redis://localhost:6379/0"
    dedup_key_parts: str = "session_id,processing_id,seq_no"
    dedup_ttl_seconds: int = 60

    # Transform
    cleaner_mode: str = "default"

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "transcription_topic"
    kafka_compression_type: Literal["none", "gzip", "snappy", "lz4"] = "none"
    kafka_send_timeout_sec: float = 10.0  # 发送超时(秒)，Kafka 不可用时超时并输出错误日志

    # Graceful shutdown timeout (seconds)
    stop_timeout: int = 120

    # Long connection: reconnect
    reconnect_enabled: bool = True
    reconnect_max_retries: int = 0  # 0 = infinite
    reconnect_initial_delay: float = 1.0
    reconnect_max_delay: float = 60.0
    reconnect_backoff_factor: float = 2.0

    # Long connection: timeouts
    sse_read_timeout: float | None = None  # None = no limit
    ws_ping_interval: float | None = 20.0  # None = disable
    ws_ping_timeout: float | None = 20.0

    # Logging (LOG_LEVEL=INFO, LOG_FORMAT=json|console|auto)
    log_level: str = "INFO"
    log_format: Literal["json", "console", "auto"] = "auto"


@lru_cache
def get_settings() -> Settings:
    """Cached settings instance."""
    return Settings()
