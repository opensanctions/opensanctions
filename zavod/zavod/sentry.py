# Forked on 2025-02-05 from
# https://github.com/kiwicom/structlog-sentry/blob/4ae68082025ca695d2fe1afa47d00b703415ca3a/structlog_sentry/__init__.py
#
# MIT License
#
# Copyright (c) 2019 Kiwi.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from __future__ import annotations

import logging
import typing

import sys
from fnmatch import fnmatch
from typing import Any, Optional
from collections.abc import MutableMapping, Iterable

from sentry_sdk import Scope, get_isolation_scope
from sentry_sdk.integrations.logging import _IGNORED_LOGGERS
from sentry_sdk.types import Event
from sentry_sdk.utils import capture_internal_exceptions, event_from_exception
from structlog.types import EventDict, ExcInfo, WrappedLogger

# See https://docs.sentry.io/concepts/data-management/event-grouping/fingerprint-rules/
SENTRY_FINGERPRINT_VARIABLE_MESSAGE = "{{ message }}"


def _figure_out_exc_info(v: Any) -> ExcInfo | tuple[None, None, None]:
    """
    Depending on the Python version will try to do the smartest thing possible
    to transform *v* into an ``exc_info`` tuple.
    """
    if isinstance(v, BaseException):
        return v.__class__, v, v.__traceback__
    elif isinstance(v, tuple):
        return v
    elif v:
        return sys.exc_info()

    return v  # type: ignore


class SentryProcessor:
    """Sentry processor for structlog.

    Uses Sentry SDK to capture events in Sentry.
    """

    def __init__(
        self,
        level: int = logging.INFO,
        event_level: int = logging.WARNING,
        active: bool = True,
        event_dict_as_extra: bool = True,
        ignore_breadcrumb_data: Iterable[str] = (
            "level",
            "logger",
            "event",
            "timestamp",
        ),
        tag_keys: list[str] | str | None = None,
        fingerprint: list[str] | None = None,
        ignore_loggers: Iterable[str] | None = None,
        verbose: bool = False,
        scope: Scope | None = None,
    ) -> None:
        """
        :param level: Events of this or higher levels will be reported as
            Sentry breadcrumbs. Dfault is :obj:`logging.INFO`.
        :param event_level: Events of this or higher levels will be reported to Sentry
            as events. Default is :obj:`logging.WARNING`.
        :param active: A flag to make this processor enabled/disabled.
        :param event_dict_as_extra: Send `event_dict` as extra info to Sentry.
            Default is :obj:`True`.
        :param ignore_breadcrumb_data: A list of data keys that will be excluded from
            breadcrumb data. Defaults to keys which are already sent separately.
        :param tag_keys: A list of keys. If any if these keys appear in `event_dict`,
            the key and its corresponding value in `event_dict` will be used as Sentry
            event tags. use `"__all__"` to report all key/value pairs of event as tags.
        :param fingerprint: A custom fingerprint for the recorded events, see
            https://docs.sentry.io/concepts/data-management/event-grouping/fingerprint-rules/.
        :param ignore_loggers: A list of logger names to ignore any events from.
        :param verbose: Report the action taken by the logger in the `event_dict`.
            Default is :obj:`False`.
        :param scope: Optionally specify :obj:`sentry_sdk.Scope`.
        """
        self.event_level = event_level
        self.level = level
        self.active = active
        self.tag_keys = tag_keys
        self.verbose = verbose

        self._scope = scope
        self._event_dict_as_extra = event_dict_as_extra
        self._original_event_dict: dict[str, Any] = {}
        self.ignore_breadcrumb_data = ignore_breadcrumb_data
        self._fingerprint = fingerprint

        self._ignored_loggers: set[str] = set()
        if ignore_loggers is not None:
            self._ignored_loggers.update(set(ignore_loggers))

    @staticmethod
    def _get_logger_name(
        logger: WrappedLogger, event_dict: MutableMapping[str, Any]
    ) -> Optional[str]:
        """Get logger name from event_dict with a fallbacks to logger.name and
        record.name

        :param logger: logger instance
        :param event_dict: structlog event_dict
        """
        record = event_dict.get("_record")
        l_name = event_dict.get("logger")
        logger_name = None

        if l_name:
            logger_name = l_name
        elif record and hasattr(record, "name"):
            logger_name = record.name

        if not logger_name and logger and hasattr(logger, "name"):
            logger_name = logger.name

        return logger_name

    def _get_scope(self) -> Scope:
        return self._scope or get_isolation_scope()

    def _get_event_and_hint(
        self, event_dict: EventDict
    ) -> tuple[Event, dict[str, Any]]:
        """Create a sentry event and hint from structlog `event_dict` and sys.exc_info.

        :param event_dict: structlog event_dict
        """
        exc_info = _figure_out_exc_info(event_dict.get("exc_info", None))
        has_exc_info = exc_info and exc_info != (None, None, None)

        if has_exc_info:
            client = self._get_scope().get_client()
            options: dict[str, Any] = client.options if client else {}
            event, hint = event_from_exception(
                exc_info,
                client_options=options,
            )
        else:
            event, hint = {}, {}

        event["message"] = str(event_dict.get("event"))
        event["level"] = event_dict.get("level")  # type: ignore
        if self._fingerprint is not None:
            event["fingerprint"] = self._fingerprint
        # We push the dataset into the logger field because that shows up in the issues overview list
        # and there doesn't seem to be a good way to change that.
        if "dataset" in event_dict:
            event["logger"] = event_dict["dataset"]

        if self._event_dict_as_extra:
            # Only add the keys we're not already reporting in other fields to extra data
            event["extra"] = {
                k: v
                for k, v in event_dict.items()
                if k not in ["event", "level", "logger"]
            }

        if self.tag_keys == "__all__":
            event["tags"] = self._original_event_dict.copy()
        if isinstance(self.tag_keys, list):
            event["tags"] = {
                key: event_dict[key] for key in self.tag_keys if key in event_dict
            }
        # Because we push the dataset into the Event.logger field (see above), we push the logger field into the tags.
        if "logger" in event_dict:
            event["tags"]["python_logger"] = event_dict["logger"]

        return event, hint

    def _get_breadcrumb_and_hint(
        self, event_dict: EventDict
    ) -> tuple[dict[str, str], dict[str, Any]]:
        data = {
            k: v for k, v in event_dict.items() if k not in self.ignore_breadcrumb_data
        }
        event = {
            "type": "log",
            "level": event_dict.get("level"),
            "category": event_dict.get("logger"),
            "message": event_dict["event"],
            "timestamp": event_dict.get("timestamp"),
            "data": data,
        }

        return event, {"log_record": event_dict}

    def _can_record(self, logger: WrappedLogger, event_dict: EventDict) -> bool:
        logger_name = self._get_logger_name(logger=logger, event_dict=event_dict)
        if logger_name:
            for ignored_logger in _IGNORED_LOGGERS | self._ignored_loggers:
                if fnmatch(logger_name, ignored_logger):
                    if self.verbose:
                        event_dict["sentry"] = "ignored"
                    return False
        return True

    def _handle_event(self, event_dict: EventDict) -> None:
        with capture_internal_exceptions():
            event, hint = self._get_event_and_hint(event_dict)
            sid = self._get_scope().capture_event(event, hint=hint)
            if sid:
                event_dict["sentry_id"] = sid
            if self.verbose:
                event_dict["sentry"] = "sent"

    def _handle_breadcrumb(self, event_dict: EventDict) -> None:
        with capture_internal_exceptions():
            event, hint = self._get_breadcrumb_and_hint(event_dict)
            self._get_scope().add_breadcrumb(event, hint=hint)

    @staticmethod
    def _get_level_value(level_name: str) -> int:
        """Get numeric value for the log level name given."""
        try:
            # Try to get one of predefined log levels
            return typing.cast(int, getattr(logging, level_name))
        except AttributeError as e:
            # May be it is a custom log level?
            level = logging.getLevelName(level_name)
            if isinstance(level, int):
                return level

            # Re-raise original error
            raise ValueError(f"{level_name} is not a valid log level") from e

    def __call__(
        self, logger: WrappedLogger, name: str, event_dict: EventDict
    ) -> EventDict:
        """A middleware to process structlog `event_dict` and send it to Sentry."""
        self._original_event_dict = dict(event_dict)
        sentry_skip = event_dict.pop("sentry_skip", False)

        if self.active and not sentry_skip and self._can_record(logger, event_dict):
            level = self._get_level_value(event_dict["level"].upper())

            if level >= self.event_level:
                self._handle_event(event_dict)

            if level >= self.level:
                self._handle_breadcrumb(event_dict)

        if self.verbose:
            event_dict.setdefault("sentry", "skipped")

        return event_dict
