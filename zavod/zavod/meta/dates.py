from functools import cached_property
import re
from typing import Dict, Any
from banal import as_bool, ensure_list, ensure_dict

from zavod.logs import get_logger

log = get_logger(__name__)


class DatesSpec(object):
    """A standardised configuration for date parsing in the context of a dataset."""

    def __init__(self, data: Dict[str, Any]) -> None:
        self.year_only = as_bool(data.get("year_only", False))
        self.formats = [str(f) for f in ensure_list(data.get("formats", []))]
        self._mappings: Dict[str, str] = {}
        months: Dict[str, Any] = ensure_dict(data.get("months", {}))
        for norm_, forms in months.items():
            norm = str(norm_)
            if len(norm) < 1:
                log.warning(f"Invalid month name: {norm}")
                continue

            for form_ in ensure_list(forms):
                form = str(form_).lower()
                if len(form) < 1:
                    log.warning(f"Invalid month name: {form}")
                    continue
                self._mappings[form] = norm

    @cached_property
    def months_re(self) -> re.Pattern[str]:
        pattern = "|".join(re.escape(m) for m in self._mappings.keys())
        pattern = f"\\b({pattern})\\b"
        return re.compile(pattern, re.IGNORECASE | re.UNICODE)
