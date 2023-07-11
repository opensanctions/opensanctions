import sys
import logging
import structlog
from typing import List, Any, Dict, MutableMapping
from structlog.stdlib import get_logger as get_raw_logger
from structlog.contextvars import merge_contextvars
from structlog.types import Processor

from zavod import settings


def configure_logging(
    level: int = logging.DEBUG,
    extra_processors: List[Processor] = [],
) -> None:
    """Configure log levels and structured logging."""
    processors: List[Processor] = [
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        merge_contextvars,
        structlog.dev.set_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]
    processors.extend(extra_processors)

    if settings.LOG_JSON:
        processors.append(structlog.processors.TimeStamper(fmt="iso"))
        processors.append(format_json)
        formatter = structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=processors,
            processor=structlog.processors.JSONRenderer(),
        )
    else:
        processors.append(structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"))
        formatter = structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=processors,
            processor=structlog.dev.ConsoleRenderer(
                exception_formatter=structlog.dev.plain_traceback
            ),
        )

    all_processors = processors + [
        structlog.stdlib.ProcessorFormatter.wrap_for_formatter
    ]

    # configuration for structlog based loggers
    structlog.configure(
        cache_logger_on_first_use=True,
        wrapper_class=structlog.stdlib.BoundLogger,
        processors=all_processors,
        logger_factory=structlog.stdlib.LoggerFactory(),
    )

    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(level)
    handler.setFormatter(formatter)

    logger = logging.getLogger()
    logger.setLevel(level)
    logger.addHandler(handler)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    return get_raw_logger(name)


def format_json(
    _: Any, __: str, ed: MutableMapping[str, str]
) -> MutableMapping[str, str]:
    """Stackdriver uses `message` and `severity` keys to display logs"""
    ed["message"] = ed.pop("event")
    ed["severity"] = ed.pop("level", "info").upper()
    return ed
