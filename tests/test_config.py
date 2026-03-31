"""Tests for ``realtime_transcribe_service.config.settings``."""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from realtime_transcribe_service.config.settings import (
    LOCAL_KAFKA_BOOTSTRAP_SERVERS,
    LOCAL_REDIS_URL,
    Settings,
)


def _settings(**kwargs: Any) -> Settings:
    """Build ``Settings`` without loading repo ``.env`` (pydantic-settings internal kwargs)."""
    return Settings(**kwargs)  # pyright: ignore[reportCallIssue]


class TestSettings:
    def test_local_defaults_fill_broker_addresses(self):
        s = _settings(_env_file=None, app_env="local")
        assert s.redis_url == LOCAL_REDIS_URL
        assert s.kafka_bootstrap_servers == LOCAL_KAFKA_BOOTSTRAP_SERVERS
        assert s.kafka_topic == "AI_STAGING_TRANSCRIPTION"
        assert s.kafka_compression_type == "zstd"
        assert s.kafka_send_timeout_sec == 2.0
        assert s.ws_ping_interval == 20.0
        assert s.ws_ping_timeout == 10.0
        assert s.stop_timeout == 120.0
        assert s.http_host == "0.0.0.0"
        assert s.http_port == 8080
        assert s.http_backlog == 4096
        assert s.http_enable_docs is False
        assert s.kafka_startup_timeout_sec == 30.0
        assert s.redis_ownership_guard_ttl_sec == 30
        assert (
            s.redis_sequence_state_key_prefix
            == "realtime-transcribe-service:expect-transcript-seq-num"
        )
        assert (
            s.redis_ownership_guard_key_prefix
            == "realtime-transcribe-service:conversation-owner"
        )
        assert s.ws_ownership_guard_refresh_interval_sec == 5.0
        assert s.log_slow_message_threshold_ms == 0.0
        assert s.auth_enabled is False
        assert s.auth_jwt_signing_material is None
        assert s.auth_jwt_algorithm == "HS256"

    def test_deployed_requires_explicit_dependency_addresses(self):
        with pytest.raises(ValidationError, match="APP_ENV=deployed"):
            _settings(_env_file=None, app_env="deployed")

    def test_stop_timeout_accepts_float(self):
        s = _settings(
            _env_file=None,
            app_env="local",
            stop_timeout=12.5,
        )
        assert s.stop_timeout == 12.5

    def test_blank_redis_url_is_rejected(self):
        with pytest.raises(ValidationError, match="must not be empty"):
            _settings(
                _env_file=None,
                app_env="local",
                redis_url="   ",
            )

    def test_unknown_env_file_key_is_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            env_file = Path(td) / "test_settings_extra.env"
            env_file.write_text("APP_ENV=local\nEXTRA_FLAG=1\n", encoding="utf-8")

            with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
                _settings(_env_file=env_file)

    def test_final_ttl_must_not_exceed_active_ttl(self):
        with pytest.raises(ValidationError, match="REDIS_FINAL_TTL_SEC"):
            _settings(
                _env_file=None,
                app_env="local",
                redis_active_ttl_sec=30,
                redis_final_ttl_sec=31,
            )

    def test_refresh_interval_must_be_less_than_guard_ttl(self):
        with pytest.raises(
            ValidationError,
            match="WS_OWNERSHIP_GUARD_REFRESH_INTERVAL_SEC must be < REDIS_OWNERSHIP_GUARD_TTL_SEC",
        ):
            _settings(
                _env_file=None,
                app_env="local",
                redis_ownership_guard_ttl_sec=5,
                ws_ownership_guard_refresh_interval_sec=5,
            )

    def test_invalid_log_level_is_rejected(self):
        with pytest.raises(ValidationError, match="log_level"):
            _settings(
                _env_file=None,
                app_env="local",
                log_level="TRACE",
            )

    def test_internal_validators_leave_non_string_values_unchanged(self):
        marker = object()

        assert Settings._normalize_app_env(marker) is marker
        assert Settings._normalize_log_level(marker) is marker
        assert Settings._normalize_auth_jwt_algorithm(marker) is marker
        assert Settings._normalize_lowercase_enums(marker) is marker
        assert Settings._reject_blank_strings(marker) is marker

    def test_auth_enabled_requires_jwt_signing_material(self):
        with pytest.raises(
            ValidationError,
            match="AUTH_JWT_SIGNING_MATERIAL must be set",
        ):
            _settings(
                _env_file=None,
                app_env="local",
                auth_enabled=True,
            )

    def test_auth_jwt_algorithm_is_normalized_to_uppercase(self):
        s = _settings(
            _env_file=None,
            app_env="local",
            auth_jwt_signing_material="signing-material",
            auth_jwt_algorithm="hs256",
        )
        assert s.auth_jwt_algorithm == "HS256"

    def test_http_enable_docs_accepts_true(self):
        s = _settings(
            _env_file=None,
            app_env="local",
            http_enable_docs=True,
        )
        assert s.http_enable_docs is True

    def test_http_enable_docs_accepts_false(self):
        s = _settings(
            _env_file=None,
            app_env="local",
            http_enable_docs=False,
        )
        assert s.http_enable_docs is False
