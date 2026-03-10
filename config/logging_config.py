"""Enterprise-grade structlog configuration.

- JSON format for production (ELK, Loki, Datadog compatible)
- Console format for local dev (TTY)
- ISO 8601 timestamps (UTC)
- Configurable via LOG_LEVEL, LOG_FORMAT
"""
import json
import logging
import os
import sys
from typing import Any, Literal
from urllib.parse import urlparse, urlunparse

import structlog


def _json_serializer(obj: Any, **kwargs: Any) -> str:
    """JSON serialize with ensure_ascii=False for readable Chinese in logs."""
    kwargs.setdefault("ensure_ascii", False)
    return json.dumps(obj, **kwargs)

SERVICE_NAME = "transcribe-service"


def _get_version() -> str:
    """Get package version for log context."""
    try:
        from importlib.metadata import version
        return version(SERVICE_NAME)
    except Exception:
        return "0.1.0"


def _add_service_context(
    logger: logging.Logger, method_name: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    """Inject service and version into every log event."""
    event_dict["service"] = SERVICE_NAME
    event_dict["version"] = _get_version()
    return event_dict


def _mask_redis_url(url: str) -> str:
    """Mask password in redis URL for safe logging."""
    if not url or "redis" not in url.lower():
        return url
    try:
        p = urlparse(url)
        if p.password or (p.username and "@" in url):
            netloc = f"{p.username or ''}:***@{p.hostname or ''}:{p.port or 6379}"
        elif p.hostname:
            netloc = f"{p.hostname}:{p.port or 6379}"
        else:
            return "redis://***"
        return urlunparse((p.scheme, netloc.rstrip(":"), p.path or "", "", "", ""))
    except Exception:
        return "redis://***"


def _mask_sensitive_processor(
    logger: logging.Logger, method_name: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    """Mask sensitive fields in log event dict."""
    for key in list(event_dict.keys()):
        key_lower = key.lower()
        if key_lower == "redis_url" and isinstance(event_dict[key], str):
            event_dict[key] = _mask_redis_url(event_dict[key])
        elif "password" in key_lower and isinstance(event_dict[key], str):
            event_dict[key] = "***"
        elif key_lower.endswith("_url") and "redis" in str(event_dict.get(key, "")).lower():
            event_dict[key] = _mask_redis_url(str(event_dict[key]))
    return event_dict


_SHARED_PROCESSORS: list[structlog.typing.Processor] = [
    _add_service_context,
    _mask_sensitive_processor,
    structlog.contextvars.merge_contextvars,
    structlog.processors.add_log_level,
    structlog.processors.TimeStamper(fmt="iso", utc=True),
    structlog.processors.StackInfoRenderer(),
    structlog.processors.format_exc_info,
    structlog.processors.UnicodeDecoder(),
]


def configure_logging(
    *,
    level: str | None = None,
    format: Literal["json", "console", "auto"] | None = None,
) -> None:
    """Configure structlog. LOG_LEVEL and LOG_FORMAT override env."""
    level = level or os.environ.get("LOG_LEVEL", "INFO").upper()
    fmt = format or os.environ.get("LOG_FORMAT", "auto").lower()

    log_level = getattr(logging, level, logging.INFO)
    if fmt == "auto":
        use_json = not sys.stderr.isatty()
    else:
        use_json = fmt == "json"

    if use_json:
        processors = _SHARED_PROCESSORS + [
            structlog.processors.dict_tracebacks,
            structlog.processors.JSONRenderer(serializer=_json_serializer),
        ]
    else:
        processors = _SHARED_PROCESSORS + [
            structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty(), pad_event=25),
        ]

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=True,
    )

    # aiokafka 连接失败时刷屏（Unable connect/Unable to update metadata），
    # 设为 CRITICAL 抑制；Kafka 不可用时由 Buffer Consumer 输出明确日志
    logging.getLogger("aiokafka").setLevel(logging.CRITICAL)


def get_logger(name: str | None = None) -> structlog.BoundLogger:
    """Get a bound logger with optional module name for traceability."""
    return structlog.get_logger(name)
