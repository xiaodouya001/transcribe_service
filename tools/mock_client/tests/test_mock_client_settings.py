"""Tests for tools.mock_client.settings."""

from __future__ import annotations

from pathlib import Path

import pytest

from tools.mock_client import settings as mock_settings


def test_get_settings_reads_prefixed_environment(monkeypatch):
    mock_settings.get_settings.cache_clear()
    mock_settings._env_file_values.cache_clear()
    monkeypatch.setenv("MOCK_CLIENT_HOST", "127.0.0.1")
    monkeypatch.setenv("MOCK_CLIENT_PORT", "9099")
    monkeypatch.setenv("MOCK_CLIENT_LOG_LEVEL", "debug")
    monkeypatch.setenv("MOCK_CLIENT_LOG_FORMAT", "json")
    monkeypatch.setenv("MOCK_CLIENT_DEFAULT_WS_URL", "ws://service.example/ws")
    monkeypatch.setenv("MOCK_CLIENT_DEFAULT_KAFKA_BOOTSTRAP", "kafka.example:9092")
    monkeypatch.setenv("MOCK_CLIENT_DEFAULT_KAFKA_TOPIC", "TOPIC_A")

    settings = mock_settings.get_settings()

    assert settings.host == "127.0.0.1"
    assert settings.port == 9099
    assert settings.log_level == "DEBUG"
    assert settings.log_format == "json"
    assert settings.default_ws_url == "ws://service.example/ws"
    assert settings.default_kafka_bootstrap == "kafka.example:9092"
    assert settings.default_kafka_topic == "TOPIC_A"


def test_get_settings_ignores_service_env_names(monkeypatch):
    mock_settings.get_settings.cache_clear()
    mock_settings._env_file_values.cache_clear()
    monkeypatch.delenv("MOCK_CLIENT_LOG_LEVEL", raising=False)
    monkeypatch.delenv("MOCK_CLIENT_LOG_FORMAT", raising=False)
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("LOG_FORMAT", "json")

    settings = mock_settings.get_settings()

    assert settings.log_level == "INFO"
    assert settings.log_format == "auto"


def test_get_settings_rejects_invalid_port(monkeypatch):
    mock_settings.get_settings.cache_clear()
    mock_settings._env_file_values.cache_clear()
    monkeypatch.setenv("MOCK_CLIENT_PORT", "70000")

    with pytest.raises(ValueError, match="MOCK_CLIENT_PORT"):
        mock_settings.get_settings()


def test_load_env_file_supports_local_dotenv():
    env_path = Path(__file__).with_name("_test_mock_client.env")
    env_path.write_text(
        "MOCK_CLIENT_LOG_LEVEL=WARNING\n"
        "MOCK_CLIENT_DEFAULT_KAFKA_TOPIC='topic-b'\n",
        encoding="utf-8",
    )

    try:
        values = mock_settings._load_env_file(env_path)
    finally:
        env_path.unlink(missing_ok=True)

    assert values["MOCK_CLIENT_LOG_LEVEL"] == "WARNING"
    assert values["MOCK_CLIENT_DEFAULT_KAFKA_TOPIC"] == "topic-b"
