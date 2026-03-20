"""Tests for config/settings.py。"""

from config.settings import Settings


class TestSettings:
    def test_defaults(self):
        s = Settings(
            _env_file=None,
            redis_url="redis://localhost:6379/0",
            kafka_bootstrap_servers="localhost:9092",
        )
        assert s.kafka_topic == "cc.transcript.realtime.v1"
        assert s.kafka_compression_type == "zstd"
        assert s.kafka_send_timeout_sec == 2.0
        assert s.ws_ping_interval == 20.0
        assert s.ws_max_size == 1048576
        assert s.stop_timeout == 120
        assert s.http_host == "0.0.0.0"
        assert s.http_port == 8080
        assert s.http_backlog == 4096
        assert s.kafka_startup_timeout_sec == 30.0
