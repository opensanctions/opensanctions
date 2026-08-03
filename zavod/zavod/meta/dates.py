import re
from typing import Any
from pydantic import BaseModel, Field, model_validator
from banal import ensure_list

from zavod.logs import get_logger

log = get_logger(__name__)


class DatesSpec(BaseModel):
    """A standardised configuration for date parsing in the context of a dataset."""

    year_only: bool = False
    formats: list[str] = []
    base_century: int | None = None
    months: dict[str | int, str | list[str]] = {}
    mappings: dict[str, str] = Field(default_factory=dict, exclude=True, init=False)
    months_re: re.Pattern[str] | None = Field(default=None, exclude=True, init=False)

    @model_validator(mode="after")
    def require_base_century_for_two_digit_years(self) -> "DatesSpec":
        for fmt in self.formats:
            if "%y" in fmt and self.base_century is None:
                msg = f"Date format {fmt!r} uses %y, which requires a base_century"
                raise ValueError(msg)
        return self

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
