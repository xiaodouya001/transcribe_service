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
from functools import lru_cache
from typing import Any, Literal
from urllib.parse import urlparse, urlunparse

import structlog


def _json_serializer(obj: Any, **kwargs: Any) -> str:
    """JSON serialize with ensure_ascii=False for readable Chinese in logs."""
    kwargs.setdefault("ensure_ascii", False)
    return json.dumps(obj, **kwargs)

SERVICE_NAME = "realtime-transcribe-service"


@lru_cache(maxsize=1)
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


def _ensure_conversation_id(
    logger: logging.Logger, method_name: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    """Ensure every log event carries a conversation_id for global search."""
    event_dict.setdefault("conversation_id", "-")
    return event_dict


def _group_identity(
    logger: logging.Logger, method_name: str, event_dict: dict[str, Any]
) -> dict[str, Any]:
    """Group fixed identity fields for cleaner console/JSON logs."""
    service = event_dict.pop("service", None)
    version = event_dict.pop("version", None)
    conversation_id = event_dict.pop("conversation_id", None)

    identity = {
        "service": service,
        "version": version,
        "conversation_id": conversation_id,
    }
    identity = {key: value for key, value in identity.items() if value is not None}
    if not identity:
        return event_dict
    return {"identity": identity, **event_dict}


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
    _ensure_conversation_id,
    _group_identity,
    structlog.stdlib.add_logger_name,
    structlog.processors.add_log_level,
    structlog.processors.TimeStamper(fmt="iso", utc=True),
    structlog.processors.StackInfoRenderer(),
    structlog.processors.format_exc_info,
    structlog.processors.UnicodeDecoder(),
]

_STRUCTLOG_PRE_PROCESSORS: list[structlog.typing.Processor] = [
    structlog.stdlib.filter_by_level,
    *_SHARED_PROCESSORS,
]

_CONSOLE_PRE_PROCESSORS: list[structlog.typing.Processor] = [
    structlog.stdlib.filter_by_level,
    *_SHARED_PROCESSORS,
]


def _configure_stdlib_logging(
    log_level: int,
    renderer: structlog.processors.JSONRenderer | structlog.dev.ConsoleRenderer,
    foreign_pre_chain: list[structlog.typing.Processor],
) -> None:
    """Route stdlib logging (including uvicorn) through the same renderer as structlog."""
    processor_formatter = structlog.stdlib.ProcessorFormatter(
        processor=renderer,
        foreign_pre_chain=foreign_pre_chain,
    )

    handler = logging.StreamHandler(stream=sys.stderr)
    handler.setFormatter(processor_formatter)
    handler.setLevel(log_level)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(log_level)

    for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "fastapi", "starlette"):
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.propagate = True
        logger.setLevel(log_level)


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
        renderer = structlog.processors.JSONRenderer(serializer=_json_serializer)
        processors = _STRUCTLOG_PRE_PROCESSORS + [
            structlog.processors.dict_tracebacks,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ]
        foreign_pre_chain = _SHARED_PROCESSORS
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty(), pad_event_to=25)
        processors = _CONSOLE_PRE_PROCESSORS + [
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ]
        foreign_pre_chain = _CONSOLE_PRE_PROCESSORS[1:]

    _configure_stdlib_logging(log_level, renderer, foreign_pre_chain)

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        context_class=dict,
        cache_logger_on_first_use=True,
    )

    # aiokafka 连接失败时刷屏（Unable connect/Unable to update metadata），
    # 设为 CRITICAL 抑制；Kafka 不可用时由 Buffer Consumer 输出明确日志
    logging.getLogger("aiokafka").setLevel(logging.CRITICAL)


def get_logger(name: str | None = None) -> structlog.BoundLogger:
    """Get a bound logger with optional module name for traceability."""
    return structlog.get_logger(name)
