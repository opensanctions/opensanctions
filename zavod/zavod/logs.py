import logging
import os
import re
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, MutableMapping, Optional

import structlog
from followthemoney.schema import Schema
from lxml.etree import _Element, tostring
from lxml.html import HtmlElement
from structlog.contextvars import merge_contextvars
from structlog.stdlib import get_logger as get_raw_logger
from structlog.types import Processor

from zavod import settings

Event = MutableMapping[str, str]


REDACT_IGNORE_LIST = {
    "OLDPWD",
    "PWD",
    "VIRTUAL_ENV",
    "HOME",
    "CLOUDSDK_CONTAINER_CLUSTER",
    "EU_JOURNAL_SEEN_PATH",
    "ZAVOD_DATA_PATH",
    "ZAVOD_OPENSANCTIONS_API_URL",
    "OPENSANCTIONS_RESOLVER_PATH",
    # The URL redaction will handle these
    "ZAVOD_DATABASE_URI",
    "OPENSANCTIONS_DATABASE_URI",
    "ZAVOD_HTTP_RETRY_TOTAL",
    "ZAVOD_HTTP_RETRY_BACKOFF_FACTOR",
    "ZAVOD_HTTP_RETRY_BACKOFF_MAX",
    "ZAVOD_XREF_MEMORY",
    "ZAVOD_XREF_THREADS",
    "NOMENKLATURA_DB_STMT_TIMEOUT",
}
REDACT_MIN_LENGTH = 5
URI_WITH_CREDENTIALS = r"(\w+)://[^:]+:[^@]+@"
REGEX_URI_WITH_CREDENTIALS = re.compile(URI_WITH_CREDENTIALS)


class RedactingProcessor:
    """A structlog processor that redact sensitive information from log messages."""

    def __init__(self, repl_pattrns: Dict[str, str | Callable[[str], str]]) -> None:
        self.repl_regexes = {re.compile(p): r for p, r in repl_pattrns.items()}

    def __call__(self, logger: Any, method_name: str, event_dict: Event) -> Event:
        return self.redact_dict(event_dict)

    def redact_dict(self, dict_: Event) -> Event:
        for key, value in dict_.items():
            if isinstance(value, str):
                value = self.redact_str(value)
            elif isinstance(value, dict):
                value = self.redact_dict(value)
            elif isinstance(value, list):
                value = self.redact_list(value)
            dict_[key] = value
        return dict_

    def redact_list(self, list_: List[Any]) -> List[Any]:
        for ix, value in enumerate(list_):
            if isinstance(value, dict):
                value = self.redact_dict(value)
            if isinstance(value, str):
                value = self.redact_str(value)
            if isinstance(value, list):
                value = self.redact_list(value)
            list_[ix] = value
        return list_

    def redact_str(self, string: str) -> str:
        for regex, replacement in self.repl_regexes.items():
            if callable(replacement):
                string = replacement(string)
            else:
                string = regex.sub(replacement, string)
        return string


def redact_uri_credentials(uri: str) -> str:
    """Redact the password from a database URI."""
    return REGEX_URI_WITH_CREDENTIALS.sub(r"\1://***:***@", uri)


def configure_redactor() -> Callable[[Any, str, Event], Event]:
    """
    Configure a redacting processor redacting env var values with some variable
    that contained that value.
    """
    pattern_map: Dict[str, str | Callable[[str], str]] = dict()
    for key, value in os.environ.items():
        if key in REDACT_IGNORE_LIST:
            continue
        if len(value) < REDACT_MIN_LENGTH:
            continue
        pattern_map[re.escape(value)] = f"${{{key}}}"
    pattern_map[URI_WITH_CREDENTIALS] = redact_uri_credentials
    return RedactingProcessor(pattern_map)


def set_logging_context_dataset_name(dataset_name: str) -> None:
    """Sets the dataset name in the logging context, so all log messages will have a dataset attached to them."""
    structlog.contextvars.bind_contextvars(dataset=dataset_name)


def configure_logging(level: int = logging.DEBUG) -> None:
    """Configure log levels and structured logging."""

    base_processors: List[Processor] = [
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        merge_contextvars,
        structlog.dev.set_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]

    emitting_processors: List[Processor] = [log_issue]

    if settings.LOG_JSON:
        formatting_processors = [
            structlog.processors.TimeStamper(fmt="iso"),
            format_json,
        ]
    else:
        formatting_processors = [
            structlog.processors.TimeStamper(
                fmt="%Y-%m-%d %H:%M:%S", utc=settings.TIME_ZONE == "UTC"
            )
        ]

    processors: List[Processor] = (
        base_processors
        + [configure_redactor()]
        + emitting_processors
        + formatting_processors
    )

    # configuration for structlog based loggers
    structlog.configure(
        cache_logger_on_first_use=True,
        wrapper_class=structlog.stdlib.BoundLogger,
        processors=processors
        + [structlog.stdlib.ProcessorFormatter.wrap_for_formatter],
        logger_factory=structlog.stdlib.LoggerFactory(),
    )

    stderr_renderer: Processor
    if settings.LOG_JSON:
        stderr_renderer = structlog.processors.JSONRenderer()
    else:
        stderr_renderer = structlog.dev.ConsoleRenderer(
            exception_formatter=structlog.dev.plain_traceback
        )

    handler = logging.StreamHandler(sys.stderr)
    handler.setLevel(level)
    handler.setFormatter(
        structlog.stdlib.ProcessorFormatter(
            # Also apply all processor for logs coming in through the standard python logging infrastructure
            foreign_pre_chain=processors,
            processor=stderr_renderer,
        )
    )

    logger = logging.getLogger()
    logger.setLevel(level)
    logger.handlers.clear()
    logger.addHandler(handler)


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    return get_raw_logger(name)


def format_json(_: Any, __: str, ed: Event) -> Event:
    """Stackdriver uses `message` and `severity` keys to display logs"""
    ed["message"] = ed.pop("event")
    ed["severity"] = ed.pop("level", "info").upper()
    return ed


def stringify(value: Any) -> Any:
    """Stringify the types that aren't already JSON serializable."""

    if isinstance(value, (_Element, HtmlElement)):
        return tostring(value, pretty_print=False, encoding=str).strip()
    if isinstance(value, Path):
        try:
            value = value.relative_to(settings.DATA_PATH)
        except ValueError:
            pass
        return str(value)
    if isinstance(value, Schema):
        return value.name
    if isinstance(value, list):
        return [stringify(v) for v in value]
    if isinstance(value, dict):
        for key, value_ in value.items():
            value[key] = stringify(value_)
    return value


def log_issue(_: Any, __: str, ed: Event) -> Event:
    data: Dict[str, Any] = stringify(dict(ed))

    context = data.pop("context", None)
    level: Optional[str] = data.get("level")
    if level is not None:
        level_num = getattr(logging, level.upper(), logging.ERROR)
        if level_num > logging.INFO and context is not None:
            from zavod.context import Context

            if isinstance(context, Context):
                if not context.dry_run:
                    context.issues.write(data)
    return data
