from enum import Enum
from typing import Optional
from datetime import datetime, timedelta
from functools import cache

from zavod.context import Context
from zavod import settings
from zavod.entity import Entity

YEAR = 365  # days
AFTER_OFFICE = 5 * YEAR
AFTER_DEATH = 5 * YEAR
MAX_AGE = 110 * YEAR
MAX_OFFICE = 40 * YEAR


class OccupancyStatus(Enum):
    CURRENT = "current"
    ENDED = "ended"
    UNKNOWN = "unknown"


@cache
def backdate(date: datetime, days: int) -> str:
    """Return a partial ISO8601 date string backdated by the number of days provided"""
    dt = date - timedelta(days=days)
    return dt.isoformat()[:10]


def occupancy_status(
    context: Context,
    person: Entity,
    position: Entity,
    no_end_implies_current: bool = True,
    current_time: datetime = settings.RUN_TIME,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    birth_date: Optional[str] = None,
    death_date: Optional[str] = None,
) -> OccupancyStatus:
    if death_date is not None and backdate(current_time, AFTER_DEATH) > death_date:
        return None

    if birth_date is not None and backdate(current_time, MAX_AGE) > birth_date:
        return None

    if not (
        death_date or birth_date or end_date or start_date or no_end_implies_current
    ):
        return None

    if end_date:
        if end_date < current_time.isoformat(): # end_date is in the past
            if end_date < backdate(current_time, AFTER_OFFICE):
                # end_date is beyond AFTER_OFFICE threshold
                return None
            else:
                # end_date is within AFTER_OFFICE threshold
                return OccupancyStatus.ENDED
        elif ( 
            context.dataset.coverage
            and context.dataset.coverage.end
            and current_time.isoformat() > context.dataset.coverage.end
        ):  # end_date is in the future and dataset is beyond its coverage.
            # Don't trust future end dates beyond the known coverage date of the dataset
            context.log.warning(
                "Future Occupancy end date is beyond the dataset coverage date. "
                "Check if the source data is being updated.",
                person=person.id,
                position=position.id,
                end_date=end_date,
            )
            return OccupancyStatus.UNKNOWN
        else: # end_date is in the future and coverage is unspecified or active
            return OccupancyStatus.CURRENT
    else:

        if start_date is not None and start_date < backdate(current_time, MAX_OFFICE):
            # No end date and start date is beyond MAX_OFFICE threshold for assuming
            # they're still a PEP.
            return None

        if no_end_implies_current:
            return OccupancyStatus.CURRENT
        else:
            return OccupancyStatus.UNKNOWN
