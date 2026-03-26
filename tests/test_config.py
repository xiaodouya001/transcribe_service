"""Tests for config/settings.py。"""

from config.settings import Settings


class TestSettings:
    def test_defaults(self):
        s = Settings(
            _env_file=None,
            redis_url="redis://127.0.0.1:6379/0",
            kafka_bootstrap_servers="127.0.0.1:9092",
        )
        assert s.kafka_topic == "AI_STAGING_TRANSCRIPTION"
        assert s.kafka_compression_type == "zstd"
        assert s.kafka_send_timeout_sec == 2.0
        assert s.ws_ping_interval == 20.0
        assert s.ws_ping_timeout == 10.0
        assert s.stop_timeout == 120
        assert s.http_host == "0.0.0.0"
        assert s.http_port == 8080
        assert s.http_backlog == 4096
        assert s.kafka_startup_timeout_sec == 30.0
        assert s.redis_ownership_guard_ttl_sec == 30
        assert s.redis_sequence_state_key_prefix == "real-time-transcriber:transcript-checker"
        assert s.redis_ownership_guard_key_prefix == "real-time-transcriber:conversation-owner"
        assert s.ws_ownership_guard_refresh_interval_sec == 5.0
        assert s.log_slow_message_threshold_ms == 0.0
