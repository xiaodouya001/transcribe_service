"""coverage: get_settings cache_clear"""

from __future__ import annotations

from realtime_transcribe_service.config.settings import get_settings


def test_get_settings_cached_then_cleared(monkeypatch):
    monkeypatch.setenv("APP_ENV", "local")
    get_settings.cache_clear()
    a = get_settings()
    b = get_settings()
    assert a is b
    get_settings.cache_clear()
    c = get_settings()
    assert c is not a
