"""coverage: config.logging_config"""

from __future__ import annotations

import json
import logging
from unittest.mock import MagicMock, patch

import pytest

import config.logging_config as lc


def test_json_serializer():
    s = lc._json_serializer({"a": "中文"})
    assert "中文" in s
    assert json.loads(s)["a"] == "中文"


def test_get_version_success():
    with patch("importlib.metadata.version", return_value="9.9.9"):
        assert lc._get_version() == "9.9.9"


def test_get_version_fallback():
    with patch("importlib.metadata.version", side_effect=Exception("no pkg")):
        assert lc._get_version() == "0.1.0"


def test_add_service_context():
    ed = {}
    out = lc._add_service_context(logging.getLogger("t"), "info", ed)
    assert out["service"] == "transcribe-service"
    assert "version" in out


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
    with patch("config.logging_config.urlparse", side_effect=Exception("boom")):
        assert lc._mask_redis_url("redis://x") == "redis://***"


def test_mask_sensitive_processor():
    ed = {
        "redis_url": "redis://u:p@h:6379/0",
        "password_field": "x",
        "some_redis_url": "redis://h:6379",
    }
    out = lc._mask_sensitive_processor(logging.getLogger("t"), "info", dict(ed))
    assert "p@" not in str(out["redis_url"])
    assert out["password_field"] == "***"


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


def test_configure_logging_invalid_level_uses_info(monkeypatch):
    monkeypatch.setenv("LOG_FORMAT", "json")
    lc.configure_logging(level="NOT_A_LEVEL")
    lc.get_logger("c").info("z")


def test_get_logger_none():
    assert lc.get_logger(None) is not None


def test_aiokafka_logger_silenced_on_configure():
    lc.configure_logging(format="json", level="DEBUG")
    assert logging.getLogger("aiokafka").level == logging.CRITICAL
