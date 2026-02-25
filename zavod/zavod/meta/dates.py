import re
from typing import Dict, Any, Optional, List
from pydantic import BaseModel, Field
from banal import ensure_list

from zavod.logs import get_logger

log = get_logger(__name__)


class DatesSpec(BaseModel):
    """A standardised configuration for date parsing in the context of a dataset."""

    year_only: bool = False
    formats: List[str] = []
    months: Dict[str | int, str | List[str]] = {}
    mappings: Dict[str, str] = Field(default_factory=dict, exclude=True, init=False)
    months_re: Optional[re.Pattern[str]] = Field(default=None, exclude=True, init=False)

    def model_post_init(self, _: Any) -> None:
        """Process months mapping after model initialization."""
        self.mappings = {}
        for norm_, forms in self.months.items():
            norm = str(norm_)
            if len(norm) < 1:
                log.warning(f"Invalid month name: {norm}")
                continue

            for form_ in ensure_list(forms):
                form = form_.lower()
                if len(form) < 1:
                    log.warning(f"Invalid month name: {form}")
                    continue
                self.mappings[form] = norm

        # Compile the regex once during initialization
        if len(self.mappings):
            pattern = "|".join(re.escape(m) for m in self.mappings.keys())
            pattern = f"\\b({pattern})\\b"
            self.months_re = re.compile(pattern, re.IGNORECASE | re.UNICODE)
