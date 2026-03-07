"""Enterprise-grade structlog configuration.

- JSON format for production (ELK, Loki, Datadog compatible)
- Console format for local dev (TTY)
- ISO 8601 timestamps (UTC)
- Configurable via LOG_LEVEL, LOG_FORMAT
"""
import logging
import os
import sys
from typing import Literal

import structlog

_SHARED_PROCESSORS: list[structlog.typing.Processor] = [
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
            structlog.processors.JSONRenderer(),
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
