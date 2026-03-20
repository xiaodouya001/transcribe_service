"""coverage: get_settings cache_clear"""

from config.settings import get_settings


def test_get_settings_cached_then_cleared():
    get_settings.cache_clear()
    a = get_settings()
    b = get_settings()
    assert a is b
    get_settings.cache_clear()
    c = get_settings()
    assert c is not a
