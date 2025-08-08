from datetime import datetime

from zavod import helpers as h
from zavod.context import Context


MAX_ENFORCEMENT_DAYS = 365 * 5


def within_max_age(
    context: Context,
    date: datetime | str,
    max_age_days: int = MAX_ENFORCEMENT_DAYS,
) -> bool:
    if isinstance(date, str):
        date = date.strip()
    date = h.extract_date(context.dataset, date)[0]
    return date > h.backdate(datetime.now(), max_age_days)
