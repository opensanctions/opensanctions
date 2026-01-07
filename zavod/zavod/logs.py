import logging
import os
import re
import sys
import banal
from rigour.env import TZ_NAME
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import structlog
from followthemoney.proxy import EntityProxy
from followthemoney.schema import Schema
from lxml.etree import _Element, tostring
from structlog.contextvars import merge_contextvars
from structlog.stdlib import get_logger as get_raw_logger
from structlog.types import Processor, EventDict as Event

from zavod import settings

REDACT_IGNORE_LIST = {
    "OLDPWD",
    "PWD",
    "VIRTUAL_ENV",
    "HOME",
    "CLOUDSDK_CONTAINER_CLUSTER",
    "EU_JOURNAL_SEEN_PATH",
    "ZAVOD_DATA_PATH",
    "ZAVOD_OPENSANCTIONS_API_URL",
    "GITHUB_JOB",
    # The URL redaction will handle these
    "ZAVOD_DATABASE_URI",
    "OPENSANCTIONS_DATABASE_URI",
    "ZAVOD_HTTP_RETRY_TOTAL",
    "ZAVOD_HTTP_RETRY_BACKOFF_FACTOR",
    "ZAVOD_HTTP_RETRY_BACKOFF_MAX",
    "NOMENKLATURA_DB_STMT_TIMEOUT",
    "NOMENKLATURA_DUCKDB_MEMORY",
    "NOMENKLATURA_DUCKDB_THREADS",
}
REDACT_MIN_LENGTH = 5
URI_WITH_CREDENTIALS = r"(\w+)://[^:]+:[^@]+@"
REGEX_URI_WITH_CREDENTIALS = re.compile(URI_WITH_CREDENTIALS)


class RedactingProcessor:
    """
    A structlog processor that redacts sensitive information from log messages.

    Patterns must be ordered such that longer/more specific patterns come first.

    While structlog copies the initial event_dict, this class also copies it because
    it needs to recurse into and modify nested structures and structlog's copy is shallow.
    """

    def __init__(self, replace_patterns: Dict[str, str | Callable[[str], str]]) -> None:
        self.repl_regexes = {re.compile(p): r for p, r in replace_patterns.items()}

    def __call__(self, logger: Any, method_name: str, event_dict: Event) -> Event:
        # The issue writer needs the context instance to write issues, so we
        # don't serialize it here, and assume the issue writer will pop it before
        # trying to write the event dict in the issue log.
        has_context_instance = False
        if "context" in event_dict:
            from zavod import Context

            if isinstance(event_dict["context"], Context):
                context = event_dict.pop("context")
                has_context_instance = True
            # else who knows what it is - redact it as normal.

        event_copy = issue_event_value(event_dict)
        redacted_event = self.redact_dict(event_copy)

        # Set it if and only if it existed before.
        if has_context_instance:
            redacted_event["context"] = context
        return redacted_event

    def redact_dict(self, dict_: Event) -> Event:
        for key, value in dict_.items():
            if isinstance(value, str):
                value = self.redact_str(value)
            elif isinstance(value, dict):
                value = self.redact_dict(value)
            elif isinstance(value, list):
                value = self.redact_list(value)
            else:
                value = self.redact_str(value)
            dict_[key] = value
        return dict_

    def redact_list(self, list_: List[Any]) -> List[Any]:
        for ix, value in enumerate(list_):
            if isinstance(value, dict):
                value = self.redact_dict(value)
            elif isinstance(value, str):
                value = self.redact_str(value)
            elif isinstance(value, list):
                value = self.redact_list(value)
            else:
                value = self.redact_str(value)
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
    env_vars_longest_first = sorted(
        os.environ.items(),
        key=lambda kv: len(kv[1]),
        reverse=True,
    )
    for key, value in env_vars_longest_first:
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


def configure_logging(level: int = logging.DEBUG) -> logging.Logger:
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
    formatting_processors: List[Processor]

    if settings.LOG_JSON:
        formatting_processors = [
            structlog.processors.TimeStamper(fmt="iso"),
            format_json,
        ]
    else:
        formatting_processors = [
            structlog.processors.TimeStamper(
                fmt="%Y-%m-%d %H:%M:%S", utc=TZ_NAME == "UTC"
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
    return logger


def reset_logging(logger: logging.Logger) -> None:
    logger.handlers.clear()


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    return get_raw_logger(name)


def format_json(_: Any, __: str, ed: Event) -> Event:
    """Stackdriver uses `message` and `severity` keys to display logs"""
    ed["message"] = ed.pop("event")
    ed["severity"] = ed.pop("level", "info").upper()
    return ed


def issue_event_value(value: Any) -> Any:
    """
    Ensure that all types are JSON-serializable and redactable,
    recursively converting everything to string, list or dict.

    This makes a deep copy of collections handled by banal.
    """

    # Do not mutate the original value

    if isinstance(value, str):
        return value
    if isinstance(value, _Element):
        return tostring(value, pretty_print=False, encoding=str).strip()
    if isinstance(value, Path):
        try:
            value = value.relative_to(settings.DATA_PATH)
        except ValueError:
            pass
        return str(value)
    if isinstance(value, Schema):
        return value.name
    if isinstance(value, EntityProxy):
        return {
            "id": value.id,
            "caption": value.caption,
            "schema": value.schema.name,
        }
    if banal.is_listish(value):
        return [issue_event_value(v) for v in value]
    if banal.is_mapping(value):
        dict_ = {}
        for key, value_ in value.items():
            dict_[key] = issue_event_value(value_)
        return dict_

    return repr(value)


def log_issue(_: Any, __: str, event_dict: Event) -> Event:
    data: Dict[str, Any] = dict(event_dict)
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
