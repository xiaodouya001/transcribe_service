"""Runtime settings for the mock-client tools."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Literal

_ENV_FILE = Path(__file__).with_name(".env")
_LOG_LEVELS = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}
_LOG_FORMATS = {"json", "console", "auto"}


def _load_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        normalized = value.strip()
        if (
            len(normalized) >= 2
            and normalized[0] == normalized[-1]
            and normalized[0] in {"'", '"'}
        ):
            normalized = normalized[1:-1]
        values[key.strip()] = normalized
    return values


@lru_cache(maxsize=1)
def _env_file_values() -> dict[str, str]:
    return _load_env_file(_ENV_FILE)


def _get_setting(name: str, default: str) -> str:
    return os.environ.get(name, _env_file_values().get(name, default))


def _require_non_empty(name: str, value: str) -> str:
    normalized = value.strip()
    if not normalized:
        raise ValueError(f"{name} must not be empty")
    return normalized


def _parse_port(name: str, value: str) -> int:
    try:
        port = int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if not (1 <= port <= 65535):
        raise ValueError(f"{name} must be in the range 1..65535")
    return port


def _parse_log_level(name: str, value: str) -> str:
    normalized = value.strip().upper()
    if normalized not in _LOG_LEVELS:
        raise ValueError(f"{name} must be one of: {', '.join(sorted(_LOG_LEVELS))}")
    return normalized


def _parse_log_format(name: str, value: str) -> Literal["json", "console", "auto"]:
    normalized = value.strip().lower()
    if normalized not in _LOG_FORMATS:
        raise ValueError(f"{name} must be one of: json, console, auto")
    return normalized  # type: ignore[return-value]


@dataclass(frozen=True)
class MockClientSettings:
    host: str
    port: int
    log_level: str
    log_format: Literal["json", "console", "auto"]
    default_ws_url: str
    default_kafka_bootstrap: str
    default_kafka_topic: str


@lru_cache(maxsize=1)
def get_settings() -> MockClientSettings:
    return MockClientSettings(
        host=_require_non_empty(
            "MOCK_CLIENT_HOST",
            _get_setting("MOCK_CLIENT_HOST", "0.0.0.0"),
        ),
        port=_parse_port(
            "MOCK_CLIENT_PORT",
            _get_setting("MOCK_CLIENT_PORT", "8088"),
        ),
        log_level=_parse_log_level(
            "MOCK_CLIENT_LOG_LEVEL",
            _get_setting("MOCK_CLIENT_LOG_LEVEL", "INFO"),
        ),
        log_format=_parse_log_format(
            "MOCK_CLIENT_LOG_FORMAT",
            _get_setting("MOCK_CLIENT_LOG_FORMAT", "auto"),
        ),
        default_ws_url=_require_non_empty(
            "MOCK_CLIENT_DEFAULT_WS_URL",
            _get_setting(
                "MOCK_CLIENT_DEFAULT_WS_URL",
                "ws://127.0.0.1:8080/ws/v1/realtime-transcriptions",
            ),
        ),
        default_kafka_bootstrap=_require_non_empty(
            "MOCK_CLIENT_DEFAULT_KAFKA_BOOTSTRAP",
            _get_setting("MOCK_CLIENT_DEFAULT_KAFKA_BOOTSTRAP", "127.0.0.1:9092"),
        ),
        default_kafka_topic=_require_non_empty(
            "MOCK_CLIENT_DEFAULT_KAFKA_TOPIC",
            _get_setting("MOCK_CLIENT_DEFAULT_KAFKA_TOPIC", "AI_STAGING_TRANSCRIPTION"),
        ),
    )
