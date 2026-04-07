"""coverage: realtime_transcribe_service.config.logging_config"""

from __future__ import annotations

import json
import logging
from unittest.mock import patch

import structlog

import realtime_transcribe_service.config.logging_config as lc


def test_json_serializer():
    s = lc._json_serializer({"a": "café"})
    assert "café" in s
    assert json.loads(s)["a"] == "café"


def test_get_version_success():
    lc._get_version.cache_clear()
    with patch("importlib.metadata.version", return_value="9.9.9"):
        assert lc._get_version() == "9.9.9"


def test_get_version_fallback():
    lc._get_version.cache_clear()
    with patch("importlib.metadata.version", side_effect=Exception("no pkg")):
        assert lc._get_version() == "1.0.0"


def test_get_version_is_cached():
    lc._get_version.cache_clear()
    with patch("importlib.metadata.version", return_value="9.9.9") as version:
        assert lc._get_version() == "9.9.9"
        assert lc._get_version() == "9.9.9"
    version.assert_called_once_with("realtime-transcribe-service")


def test_add_service_context():
    ed = {}
    out = lc._add_service_context(logging.getLogger("t"), "info", ed)
    assert out["service"] == "realtime-transcribe-service"
    assert "version" in out


def test_group_identity():
    ed = {
        "service": "realtime-transcribe-service",
        "version": "1.0.0",
        "conversation_id": "conv-1",
        "event": "ready",
    }
    out = lc._group_identity(logging.getLogger("t"), "info", ed)
    assert out["identity"] == {
        "service": "realtime-transcribe-service",
        "version": "1.0.0",
        "conversation_id": "conv-1",
    }
    assert out["event"] == "ready"
    assert "service" not in out
    assert "version" not in out
    assert "conversation_id" not in out
    assert list(out.keys())[-1] == "identity"


def test_group_identity_without_fixed_fields_returns_original():
    ed = {"event": "ready"}
    out = lc._group_identity(logging.getLogger("t"), "info", dict(ed))
    assert out == ed


def test_redact_text_for_logs_masks_embedded_url_and_secret():
    t = lc.redact_text_for_logs(
        "boom redis://u:XYZSECRET123@host:6379/0 tail XYZSECRET123",
        extra_secret="XYZSECRET123",
    )
    assert "XYZSECRET123" not in t


def test_mask_redis_url_empty():
    assert lc._mask_redis_url("") == ""


def test_mask_redis_url_not_redis():
    assert lc._mask_redis_url("http://x") == "http://x"


def test_mask_redis_url_with_password():
    u = "redis://user:secret@redis.example.com:6379/0"
    m = lc._mask_redis_url(u)
    assert "secret" not in m
    assert "***" in m


def test_mask_redis_url_hostname_only():
    m = lc._mask_redis_url("redis://127.0.0.1:6379/0")
    assert "127.0.0.1" in m


def test_mask_redis_url_no_hostname_fallback():
    assert lc._mask_redis_url("redis://:6379/0") == "redis://***"


def test_mask_redis_url_parse_error():
    with patch(
        "realtime_transcribe_service.config.logging_config.urlparse",
        side_effect=Exception("boom"),
    ):
        assert lc._mask_redis_url("redis://x") == "redis://***"


def test_mask_sensitive_processor():
    ed = {
        "redis_url": "redis://u:p@h:6379/0",
        "password_field": "x",
        "some_redis_url": "redis://h:6379",
        "error": "fail redis://u:secret@h:6379/0",
    }
    out = lc._mask_sensitive_processor(logging.getLogger("t"), "info", dict(ed))
    assert "p@" not in str(out["redis_url"])
    assert "secret" not in str(out["error"])
    assert out["password_field"] == "***"


def test_env_flag_enabled_and_normalize_access_path(monkeypatch):
    monkeypatch.delenv("SUPPRESS_HEALTH_ACCESS_LOGS", raising=False)
    assert lc._env_flag_enabled("SUPPRESS_HEALTH_ACCESS_LOGS", default=True) is True

    monkeypatch.setenv("SUPPRESS_HEALTH_ACCESS_LOGS", " On ")
    assert lc._env_flag_enabled("SUPPRESS_HEALTH_ACCESS_LOGS") is True
    assert lc._normalize_access_path(None) is None
    assert lc._normalize_access_path("/ready/?probe=1") == "/ready"
    assert lc._normalize_access_path("/") == "/"


def test_extract_uvicorn_access_path_and_filter_fallbacks():
    non_access = logging.LogRecord("app", logging.INFO, __file__, 1, "hello", (), None)
    assert lc._extract_uvicorn_access_path(non_access) is None

    unparsable = logging.LogRecord(
        "uvicorn.access",
        logging.INFO,
        __file__,
        1,
        "not an access log line",
        (),
        None,
    )
    assert lc._extract_uvicorn_access_path(unparsable) is None

    regex_fallback = logging.LogRecord(
        "uvicorn.access",
        logging.INFO,
        __file__,
        1,
        '127.0.0.1:1234 - "GET /transcribe-svc/ready HTTP/1.1" 200',
        (),
        None,
    )
    assert lc._extract_uvicorn_access_path(regex_fallback) == "/transcribe-svc/ready"

    access_filter = lc._SuppressHealthAccessFilter()
    assert access_filter.filter(non_access) is True
    assert access_filter.filter(regex_fallback) is False


def test_configure_logging_console(monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("LOG_FORMAT", "console")
    with patch.object(lc.sys.stderr, "isatty", return_value=False):
        lc.configure_logging()
    log = lc.get_logger("test_mod")
    log.info("hello")


def test_configure_logging_json_explicit(monkeypatch):
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    monkeypatch.delenv("LOG_FORMAT", raising=False)
    lc.configure_logging(level="WARNING", format="json")
    lc.get_logger("j").warning("w")


def test_configure_logging_stdlib_logger_json(monkeypatch, capsys):
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    monkeypatch.delenv("LOG_FORMAT", raising=False)
    lc.configure_logging(level="INFO", format="json")
    logging.getLogger("uvicorn.access").info("GET /health 200")
    out = capsys.readouterr().err
    assert '"identity": {' in out
    assert '"service": "realtime-transcribe-service"' in out
    assert '"logger": "uvicorn.access"' in out
    assert '"conversation_id": "-"' in out
    assert "GET /health 200" in out


def test_configure_logging_can_suppress_health_access_logs(monkeypatch, capsys):
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    monkeypatch.delenv("LOG_FORMAT", raising=False)
    lc.configure_logging(
        level="INFO",
        format="json",
        suppress_health_access_logs=True,
    )
    logger = logging.getLogger("uvicorn.access")
    logger.info(
        '%s - "%s %s HTTP/%s" %d',
        "127.0.0.1:1234",
        "GET",
        "/transcribe-svc/health",
        "1.1",
        200,
    )
    logger.info(
        '%s - "%s %s HTTP/%s" %d',
        "127.0.0.1:1234",
        "GET",
        "/metrics",
        "1.1",
        200,
    )
    out = capsys.readouterr().err
    assert "/transcribe-svc/health" not in out
    assert "/metrics" in out


def test_configure_logging_structlog_json_includes_conversation_id(monkeypatch, capsys):
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    monkeypatch.delenv("LOG_FORMAT", raising=False)
    lc.configure_logging(level="INFO", format="json")
    lc.get_logger("app").info("ready")
    out = capsys.readouterr().err.strip()
    payload = json.loads(out)
    assert payload["identity"]["service"] == "realtime-transcribe-service"
    assert payload["identity"]["conversation_id"] == "-"
    assert payload["event"] == "ready"
    assert "service" not in payload
    assert "conversation_id" not in payload
    assert list(payload.keys())[-1] == "identity"


def test_configure_logging_auto_tty_json(monkeypatch):
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    monkeypatch.delenv("LOG_FORMAT", raising=False)
    with patch.object(lc.sys.stderr, "isatty", return_value=False):
        lc.configure_logging(format="auto")
    lc.get_logger("a").info("x")


def test_configure_logging_auto_tty_console(monkeypatch):
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    monkeypatch.delenv("LOG_FORMAT", raising=False)
    with patch.object(lc.sys.stderr, "isatty", return_value=True):
        lc.configure_logging(format="auto")
    lc.get_logger("b").info("y")


def test_configure_logging_console_groups_identity(monkeypatch, capsys):
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    monkeypatch.delenv("LOG_FORMAT", raising=False)
    with patch.object(lc.sys.stderr, "isatty", return_value=False):
        lc.configure_logging(format="console", level="INFO")
    lc.get_logger("console_app").info("ready", conversation_id="conv-1")
    out = capsys.readouterr().err
    assert "identity={'service': 'realtime-transcribe-service'" in out
    assert "'conversation_id': 'conv-1'" in out
    assert " conversation_id=" not in out


def test_configure_logging_invalid_level_uses_info(monkeypatch):
    monkeypatch.setenv("LOG_FORMAT", "json")
    lc.configure_logging(level="NOT_A_LEVEL")
    lc.get_logger("c").info("z")


def test_get_logger_none():
    assert lc.get_logger(None) is not None


def test_aiokafka_logger_follows_debug_on_configure():
    lc.configure_logging(format="json", level="DEBUG")
    assert logging.getLogger("aiokafka").level == logging.DEBUG


def test_aiokafka_logger_silenced_on_non_debug_configure():
    lc.configure_logging(format="json", level="INFO")
    assert logging.getLogger("aiokafka").level == logging.CRITICAL


def test_configure_logging_filters_debug_before_expensive_processors(monkeypatch):
    seen: list[tuple[str, str | None]] = []

    def touch(logger, method_name, event_dict):
        seen.append((method_name, event_dict.get("event")))
        return event_dict

    monkeypatch.setattr(
        lc,
        "_STRUCTLOG_PRE_PROCESSORS",
        [structlog.stdlib.filter_by_level, touch],
    )
    lc.configure_logging(level="INFO", format="json")

    log = lc.get_logger("perf_guard")
    log.debug("hidden")
    log.info("visible")

    assert seen == [("info", "visible")]

