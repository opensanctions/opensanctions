import sys
import logging
import structlog
from pathlib import Path
from lxml.etree import _Element, tostring
from structlog.contextvars import merge_contextvars
from followthemoney.schema import Schema

from opensanctions import settings


def store_event(logger, log_method, data):
    for key, value in data.items():
        if isinstance(value, _Element):
            value = tostring(value, pretty_print=False, encoding=str)
        if isinstance(value, Path):
            value = str(value.relative_to(settings.DATA_PATH))
        if isinstance(value, Schema):
            value = value.name
        data[key] = value

    level_num = getattr(logging, data.get("level").upper())
    if level_num > logging.INFO and "dataset" in data:
        Issue.save(data)
    return data


def configure_logging(level=logging.DEBUG):
    """Configure log levels and structured logging"""
    processors = [
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S"),
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        merge_contextvars,
        store_event,
    ]
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=list(processors),
        processor=structlog.dev.ConsoleRenderer(),
    )

    processors.append(structlog.stdlib.ProcessorFormatter.wrap_for_formatter)

    # configuration for structlog based loggers
    structlog.configure(
        processors=processors,
        logger_factory=structlog.stdlib.LoggerFactory(),
    )

    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(level)
    handler.setFormatter(formatter)

    logger = logging.getLogger()
    logger.setLevel(level)
    logger.addHandler(handler)
