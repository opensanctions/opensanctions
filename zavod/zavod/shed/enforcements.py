from datetime import datetime

from zavod import helpers as h
from zavod.context import Context
from zavod.settings import RUN_TIME


MAX_ENFORCEMENT_DAYS = 365 * 5


def within_max_age(
    context: Context,
    date: datetime | str,
    max_age_days: int = MAX_ENFORCEMENT_DAYS,
) -> bool:
    """
    Check if an enforcement date is within the maximum age of enforcement actions.

    Args:
        context: The runner context with dataset metadata.
        date: The enforcement date to check.
        max_age_days: The maximum age of enforcement actions in days, if different from the default.
    """
    if isinstance(date, str):
        date = date.strip()
    cleaned_date = h.extract_date(context.dataset, date, fallback_to_original=False)[0]
    return cleaned_date > h.backdate(RUN_TIME, max_age_days)
