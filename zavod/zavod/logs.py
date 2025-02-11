import logging
import os
import re
from pathlib import Path
from typing import Callable, Optional
from typing import Dict, List, Any, MutableMapping

import structlog
import sys
from lxml.etree import _Element, tostring
from lxml.html import HtmlElement
from structlog.contextvars import merge_contextvars
from structlog.stdlib import get_logger as get_raw_logger
from structlog.types import Processor

import sentry_sdk
from followthemoney.schema import Schema
from zavod import settings
from zavod.sentry import SentryProcessor, SENTRY_FINGERPRINT_VARIABLE_MESSAGE

Event = MutableMapping[str, str]


REDACT_IGNORE_LIST = {
    "OLDPWD",
    "PWD",
    "VIRTUAL_ENV",
    "HOME",
    "EU_JOURNAL_SEEN_PATH",
    "ZAVOD_DATA_PATH",
    "ZAVOD_RESOLVER_PATH",
    "ZAVOD_SYNC_POSITIONS",
    "OPENSANCTIONS_RESOLVER_PATH",
    # The URL redaction will handle these
    "ZAVOD_DATABASE_URI",
    "OPENSANCTIONS_DATABASE_URI",
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


def configure_sentry_integration() -> None:
    if settings.ENABLE_SENTRY:
        if not settings.SENTRY_DSN:
            raise RuntimeError("Sentry integration is enabled, but not DSN set.")
        if not settings.SENTRY_ENVIRONMENT:
            raise RuntimeError("Sentry integration is enabled, but no environment set.")

        sentry_sdk.init(
            dsn=settings.SENTRY_DSN,
            environment=settings.SENTRY_ENVIRONMENT,
            auto_enabling_integrations=False,
            disabled_integrations=[
                # We disable the default logging integration because we have our custom structlog Processor
                sentry_sdk.integrations.logging.LoggingIntegration  # type: ignore
            ],
        )


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
    if settings.ENABLE_SENTRY:
        sentry_processor = SentryProcessor(
            event_level=logging.WARNING,
            event_dict_as_extra=True,
            # Attach the dataset name as a tag in Sentry
            tag_keys=["dataset"],
            # Disable the default grouping magic, we only want to group by message and dataset. Otherwise, log messages
            # along the lines of "Problem with entity <id>" might be grouped by the internal magic, even though they are
            # actually separate data issues.
            fingerprint=[SENTRY_FINGERPRINT_VARIABLE_MESSAGE, "{{ tag.dataset }}"],
        )
        processors.append(sentry_processor)

    # Note: Redaction is only happening on string values, so make sure production
    # environments format logs as strings before the redaction processor.
    if settings.LOG_JSON:
        processors.append(structlog.processors.TimeStamper(fmt="iso"))
        processors.append(format_json)
        formatter = structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=processors,
            processor=structlog.processors.JSONRenderer(),
        )
    else:
        processors.append(
            structlog.processors.TimeStamper(
                fmt="%Y-%m-%d %H:%M:%S", utc=settings.TIME_ZONE == "UTC"
            )
        )
        formatter = structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=processors,
            processor=structlog.dev.ConsoleRenderer(
                exception_formatter=structlog.dev.plain_traceback
            ),
        )

    all_processors = processors + [
        configure_redactor(),
        structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
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
