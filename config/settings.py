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
    )

    # Fanolab ASR
    fanolab_url: str = "http://localhost:8765/sse"
    mode: Literal["sse", "websocket"] = "sse"

    # Redis
    redis_url: str = "redis://localhost:6379/0"
    dedup_key_parts: str = "session_id,processing_id,seq_no"

    # Redis buffer (when redis_buffer_enabled)
    redis_buffer_enabled: bool = True
    redis_buffer_stream: str = "asr:ingest:buffer"
    redis_buffer_consumer_group: str = "asr:ingest:consumer"
    redis_buffer_maxlen: int = 10000

    # Transform
    cleaner_mode: str = "default"

    # Kafka
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_topic: str = "asr_realtime_text"

    # Demo mode: MemoryDedup + EchoProducer when True
    demo_mode: bool = True

    # Graceful shutdown timeout (seconds)
    stop_timeout: int = 120


@lru_cache
def get_settings() -> Settings:
    """Cached settings instance."""
    return Settings()
