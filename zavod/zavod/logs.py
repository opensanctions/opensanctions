import sys
import logging
import structlog
from pathlib import Path
from lxml.etree import _Element, tostring
from followthemoney.schema import Schema

from typing import Dict, List, Any, MutableMapping
from structlog.stdlib import get_logger as get_raw_logger
from structlog.contextvars import merge_contextvars
from structlog.types import Processor

from zavod import settings

Event = MutableMapping[str, str]


def configure_logging(level: int = logging.DEBUG) -> None:
    """Configure log levels and structured logging."""
    processors: List[Processor] = [
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        merge_contextvars,
        structlog.dev.set_exc_info,
        structlog.processors.UnicodeDecoder(),
        log_issue,
    ]

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


def format_json(_: Any, __: str, ed: Event) -> Event:
    """Stackdriver uses `message` and `severity` keys to display logs"""
    ed["message"] = ed.pop("event")
    ed["severity"] = ed.pop("level", "info").upper()
    return ed


def log_issue(_: Any, __: str, ed: Event) -> Event:
    data: Dict[str, Any] = dict(ed)
    for key, value in data.items():
        if isinstance(value, _Element):
            value = tostring(value, pretty_print=False, encoding=str)
        if isinstance(value, Path):
            try:
                value = value.relative_to(settings.DATA_PATH)
            except ValueError:
                pass
            value = str(value)
        if isinstance(value, Schema):
            value = value.name
        data[key] = value

    context = data.pop("context", None)
    level = data.get("level")
    if level is not None:
        level_num = getattr(logging, level.upper())
        if level_num > logging.INFO and context is not None:
            from zavod.context import Context

            if isinstance(context, Context):
                if not context.dry_run:
                    context.issues.write(data)
    return data
