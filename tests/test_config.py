"""Tests for config/settings."""

import pytest

from config.settings import Settings, get_settings


def test_settings_defaults() -> None:
    """Settings has expected default values."""
    s = Settings(_env_file=None)  # Bypass .env to test defaults
    assert s.stt_provider_url == "http://localhost:8765/sse"
    assert s.mode == "sse"
    assert s.redis_url == "redis://localhost:6379/0"
    assert s.kafka_bootstrap_servers == "localhost:9092"
    assert s.kafka_topic == "transcription_topic"
    assert s.kafka_compression_type == "none"
    assert s.kafka_send_timeout_sec == 10.0
    assert s.redis_buffer_stream == "transcription:ingest:buffer"
    assert s.reconnect_enabled is True


def test_get_settings_cached() -> None:
    """get_settings returns same instance (cached)."""
    a = get_settings()
    b = get_settings()
    assert a is b
