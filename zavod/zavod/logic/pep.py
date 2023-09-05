from enum import Enum
from typing import Optional
from datetime import datetime

from zavod.context import Context
from zavod import settings
from zavod import helpers as h

YEAR = 365  # days
AFTER_OFFICE = 5 * YEAR
AFTER_DEATH = 5 * YEAR
MAX_AGE = 110 * YEAR
MAX_OFFICE = 40 * YEAR


class OccupancyStatus(Enum):
    CURRENT = "current"
    ENDED = "ended"
    UNKNOWN = "unknown"


def occupancy_status(
    context: Context,
    no_end_implies_current: bool = True,
    current_time: datetime = settings.RUN_TIME,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    birth_date: Optional[str] = None,
    death_date: Optional[str] = None,
) -> OccupancyStatus:
    if death_date is not None and h.backdate(current_time, AFTER_DEATH) > death_date:
        return None

    if birth_date is not None and h.backdate(current_time, MAX_AGE) > birth:
        return False

    if end_date is not None and h.backdate(current_time, AFTER_OFFICE) > end_date:
        return None

    if start_date is not None and h.backdate(current_time, MAX_OFFICE) > start_date:
        return None

    if not death_date or birth_date or end_date or start_date or no_end_implies_current:
        return None

    if end_date:
        if end_date < current_time.isoformat():
            return OccupancyStatus.ENDED
        elif (
            context.dataset.coverage
            and context.dataset.coverage.end
            and current_time.isoformat() > context.dataset.coverage.end
        ):
            # Don't trust future end dates beyond the known coverage date of the dataset
            context.log.warning(
                "Future Occupancy end date is beyond the dataset coverage date. "
                "Check if the source data is being updated.",
                person=person.id,
                position=position.id,
                end_date=end_date,
            )
            return OccupancyStatus.UNKNOWN
        else:
            return OccupancyStatus.CURRENT
    else:
        if no_end_implies_current:
            return OccupancyStatus.CURRENT
        else:
            return OccupancyStatus.UNKNOWN
