from enum import Enum
from typing import Optional, List
from datetime import datetime, timedelta
from functools import cache

from zavod.context import Context
from zavod import settings
from zavod.entity import Entity

YEAR = 365  # days
DEFAULT_AFTER_OFFICE = 5 * YEAR
EXTENDED_AFTER_OFFICE = 20 * YEAR
AFTER_DEATH = 5 * YEAR
MAX_AGE = 110 * YEAR
MAX_OFFICE = 40 * YEAR


class OccupancyStatus(Enum):
    CURRENT = "current"
    ENDED = "ended"
    UNKNOWN = "unknown"


class PositionCategorisation:
    def __init__(self, topics: List[str], is_pep: Optional[bool]):
        self.topics = topics
        self.is_pep = is_pep


@cache
def categorise(
    context: Context,
    position: Entity,
    is_pep: Optional[bool] = True,
) -> PositionCategorisation:
    """Checks whether this is a PEP position and for any topics needed to make
    PEP duration decisions.

    If the position is not in the database yet, it is added.

    Only emit positions where is_pep is true, even if the crawler sets is_pep
    to true, in case is_pep has been changed to false in the database.

    Args:
      context:
      position: The position to be categorised
      is_pep: Initial value for is_pep in the database if it gets added.
    """
    if settings.OPENSANCTIONS_API_KEY is None:
        context.log.warning(
            (
                "OPENSANCTIONS_API_KEY not configured. Can't check "
                f"{position.get('country')} {position.get('name')}"
            )
        )
        return PositionCategorisation(topics=[], is_pep=None)

    url = f"{settings.OPENSANCTIONS_API_URL}/positions/{position.id}"
    headers = {"authorization": settings.OPENSANCTIONS_API_KEY}
    res = context.http.get(url, headers=headers)

    if res.status_code == 200:
        data = res.json()
    elif res.status_code == 404:
        context.log.info("Adding position not yet in database", entity_id=position.id)
        url = f"{settings.OPENSANCTIONS_API_URL}/positions/"
        body = {
            "entity_id": position.id,
            "caption": position.caption,
            "countries": position.get("country"),
            "topics": position.get("topics"),
            "is_pep": is_pep,
        }
        res = context.http.post(url, headers=headers, json=body)
        res.raise_for_status()
        data = res.json()
    else:
        res.raise_for_status()

    if data.get("is_pep") is None:
        context.log.debug(
            (
                f'Position {position.get("country")} {position.get("name")}'
                " not yet categorised as PEP or not."
            )
        )

    return PositionCategorisation(
        topics=data.get("topics", []),
        is_pep=data.get("is_pep"),
    )


@cache
def backdate(date: datetime, days: int) -> str:
    """Return a partial ISO8601 date string backdated by the number of days provided"""
    dt = date - timedelta(days=days)
    return dt.isoformat()[:10]


def get_after_office(topics: List[str]) -> int:
    if "gov.national" in topics:
        return EXTENDED_AFTER_OFFICE
    if "gov.igo" in topics:
        return EXTENDED_AFTER_OFFICE
    return DEFAULT_AFTER_OFFICE


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
    categorisation: Optional[PositionCategorisation] = None,
) -> Optional[OccupancyStatus]:
    if death_date is not None and death_date < backdate(current_time, AFTER_DEATH):
        # If they did longer ago than AFTER_DEATH threshold, don't consider a PEP.
        return None

    if birth_date is not None and birth_date < backdate(current_time, MAX_AGE):
        # If they're unrealistically old, assume they're not a PEP.
        return None

    if not (
        death_date or birth_date or end_date or start_date or no_end_implies_current
    ):
        # If we don't have any dates to work with, nor a really well-maintained source,
        # don't consider them a PEP.
        return None

    if categorisation is None:
        topics = position.get("topics")
    else:
        topics = categorisation.topics

    if end_date:
        if end_date < current_time.isoformat():  # end_date is in the past
            after_office = get_after_office(topics)
            if end_date < backdate(current_time, after_office):
                # end_date is beyond after-office threshold
                return None
            else:
                # end_date is within after-office threshold
                return OccupancyStatus.ENDED
        elif (
            context.dataset.coverage
            and context.dataset.coverage.end
            and context.dataset.coverage.end < current_time.isoformat()
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
        else:  # end_date is in the future and coverage is unspecified or active
            return OccupancyStatus.CURRENT

    else:  # No end date
        if start_date is not None and start_date < backdate(current_time, MAX_OFFICE):
            # start_date is older than MAX_OFFICE threshold for assuming they're still
            # a PEP
            return None

        if no_end_implies_current:
            # This is for sources we are really confident will provide an end date
            # or totally remove the person soon enough after the person leaves the
            # position
            return OccupancyStatus.CURRENT
        else:
            return OccupancyStatus.UNKNOWN
