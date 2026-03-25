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
    redis_sequence_state_key_prefix: str = "transcript:session"
    redis_ownership_guard_key_prefix: str = "transcript:owner"

    # --- Kafka ---
    kafka_bootstrap_servers: str = "127.0.0.1:9092"
    kafka_topic: str = "cc.transcript.realtime.v1"
    kafka_topic_num_partitions: int = 50
    kafka_replication_factor: int = 1
    kafka_compression_type: Literal["none", "gzip", "snappy", "lz4", "zstd"] = "zstd"
    kafka_send_timeout_sec: float = 2.0
    kafka_linger_ms: int = 1
    kafka_batch_size: int = 32768

    # --- WebSocket ---
    # 传入 Uvicorn `ws="websockets"`：服务端按间隔发 RFC Ping、依赖对端 Pong（见 main.py）
    ws_ping_interval: float = 20.0
    ws_ping_timeout: float = 20.0
    # ownership guard 续租周期（秒），仅用于连接存活期间后台 refresh
    ws_ownership_guard_refresh_interval_sec: float = 5.0

    # --- HTTP / Uvicorn ---
    http_host: str = "0.0.0.0"
    http_port: int = 8080
    # listen() backlog；瞬时大量建连时过小可能导致对端在读到 101 前被 RST（默认 2048）
    http_backlog: int = 4096
    # 最大同时在线 WebSocket 连接数；超出后新连接以 1013 拒绝。0 = 不限制
    ws_max_connections: int = 0

    # --- Startup ---
    kafka_startup_timeout_sec: float = 30.0

    # --- Graceful shutdown ---
    stop_timeout: int = 120

    # --- Logging ---
    log_level: str = "INFO"
    log_format: Literal["json", "console", "auto"] = "auto"
    # 是否打印服务端发出的 ERROR 响应完整 JSON（默认关闭，避免压测日志过大）
    log_ws_error_frames: bool = False


@lru_cache
def get_settings() -> Settings:
    """Cached settings instance."""
    return Settings()
