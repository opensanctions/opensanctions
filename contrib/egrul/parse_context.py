from dataclasses import dataclass
from datetime import date
from typing import Optional

import structlog

from zavod import Context


@dataclass(frozen=True)
class ParseContext:
    """Per-XML-file parsing context: which archive member we're in, and when."""

    origin: str
    data_time: date
    # TODO: Heavy dependency for what we use it for — entity IDs and logging.
    _context: Context

    @property
    def log(self) -> structlog.stdlib.BoundLogger:
        return self._context.log

    def make_id(self, *parts: Optional[str]) -> Optional[str]:
        return self._context.make_id(*parts)

    def make_slug(self, *parts: Optional[str]) -> Optional[str]:
        return self._context.make_slug(*parts)
