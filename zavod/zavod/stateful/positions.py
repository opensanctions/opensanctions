from datetime import datetime, timedelta
from enum import Enum
from functools import lru_cache
from typing import List, Optional

from rigour.dates import ended_before, starts_after
from rigour.ids.wikidata import is_qid
from sqlalchemy import select

from zavod import settings
from zavod.context import Context
from zavod.entity import Entity
from zavod.stateful.model import position_table

NOTIFIED_SYNC_POSITIONS = False

YEAR_DAYS = 365  # days
DEFAULT_AFTER_OFFICE = timedelta(days=5 * YEAR_DAYS)
EXTENDED_AFTER_OFFICE_YEARS = 20
EXTENDED_AFTER_OFFICE = timedelta(days=EXTENDED_AFTER_OFFICE_YEARS * YEAR_DAYS)
NO_EXPIRATION = timedelta(days=50 * YEAR_DAYS)
AFTER_DEATH = timedelta(days=5 * YEAR_DAYS)
MAX_AGE = timedelta(days=110 * YEAR_DAYS)
MAX_OFFICE = timedelta(days=40 * YEAR_DAYS)


class OccupancyStatus(Enum):
    CURRENT = "current"
    ENDED = "ended"
    UNKNOWN = "unknown"


class PositionCategorisation(object):
    is_pep: Optional[bool]
    """Whether the position denotes a politically exposed person or not"""
    topics: List[str]
    """The topics linked to the position, as a list"""

    __slots__ = ["topics", "is_pep"]

    def __init__(self, topics: List[str], is_pep: Optional[bool]):
        self.topics = topics
        self.is_pep = is_pep


@lru_cache(maxsize=2000)
def categorise(
    context: Context,
    position: Entity,
    *,
    default_is_pep: Optional[bool] = True,
) -> PositionCategorisation:
    """Return the reviewed categorisation (topics, is_pep) for a position.

    If the position has already been reviewed, the reviewed values are
    returned and override `default_is_pep` and the topics on the entity.
    Otherwise, the position is enrolled into the review UI with the
    crawler-supplied topics and `default_is_pep`, and those defaults are
    returned; later calls will pick up any edits made through the UI.
    """
    countries = sorted(position.get("country"))
    subnational_areas = sorted(position.get("subnationalArea"))
    stmt = position_table.select()
    stmt = stmt.filter(position_table.c.entity_id == position.id)
    stmt = stmt.filter(position_table.c.deleted_at.is_(None))
    for row in context.db.execute(stmt).fetchall():
        if (
            row.caption != position.caption
            or sorted(row.countries) != countries
            or sorted(row.subnational_areas or []) != subnational_areas
        ):
            # If the caption or countries have changed, we need to update the row.
            context.log.info(
                "Updating position metadata",
                entity_id=position.id,
                caption=position.caption,
                countries=countries,
                subnational_areas=subnational_areas,
            )
            ustmt = position_table.update()
            ustmt = ustmt.where(position_table.c.id == row.id)
            updates = {
                "caption": position.caption,
                "countries": countries,
                "subnational_areas": subnational_areas,
                # "modified_at": settings.RUN_TIME,
            }
            ustmt = ustmt.values(updates)
            context.db.execute(ustmt)
        return PositionCategorisation(
            topics=row.topics,
            is_pep=row.is_pep,
        )

    body = {
        "entity_id": position.id,
        "caption": position.caption,
        "countries": countries,
        "subnational_areas": subnational_areas,
        "topics": position.get("topics"),
        "dataset": position.dataset.name,
        "created_at": settings.RUN_TIME,
        "is_pep": default_is_pep,
    }
    istmt = position_table.insert()
    istmt = istmt.values(body)
    context.db.execute(istmt)
    return PositionCategorisation(topics=position.get("topics"), is_pep=default_is_pep)


def categorise_many(
    contextL: Context, position_ids: List[str]
) -> List[PositionCategorisation]:
    """Categorise multiple positions at once. This is a performance optimisation to
    avoid multiple database queries."""
    stmt = position_table.select()
    stmt = stmt.filter(position_table.c.entity_id.in_(position_ids))
    stmt = stmt.filter(position_table.c.deleted_at.is_(None))
    rows = contextL.db.execute(stmt).fetchall()
    categorisations = []
    for row in rows:
        categorisations.append(
            PositionCategorisation(
                topics=row.topics,
                is_pep=row.is_pep,
            )
        )
    return categorisations


def categorised_position_qids(context: Context) -> List[str]:
    """Return a list of position QIDs that have been categorised."""
    stmt = select(position_table.c.entity_id)
    stmt = stmt.filter(position_table.c.is_pep.is_(True))
    stmt = stmt.filter(position_table.c.deleted_at.is_(None))
    stmt = stmt.filter(position_table.c.entity_id.like("Q%"))
    rows = context.db.execute(stmt).fetchall()
    qids = []
    for row in rows:
        if is_qid(row.entity_id):
            qids.append(row.entity_id)
    return qids


def get_after_office(topics: List[str]) -> timedelta:
    if "gov.national" in topics:
        if "gov.head" in topics:
            return NO_EXPIRATION
        return EXTENDED_AFTER_OFFICE
    if "gov.igo" in topics or "role.diplo" in topics:
        return EXTENDED_AFTER_OFFICE
    return DEFAULT_AFTER_OFFICE


def occupancy_status(
    context: Context,
    *,
    person: Entity,
    position: Entity,
    occupancy: Entity,
    no_end_implies_current: bool = True,
    current_time: datetime = settings.RUN_TIME,
    birth_date: Optional[str] = None,
    death_date: Optional[str] = None,
    categorisation: Optional[PositionCategorisation] = None,
) -> Optional[OccupancyStatus]:
    """Determine the occupancy status of a person in a position given a set of dates.

    Dates are extracted from the occupancy entity. The effective start date is
    determined by checking startDate, periodStart, and electionDate in that order.
    End dates are handled with different semantics depending on specificity:

    - ``endDate`` (individual end): past implies ENDED, future implies CURRENT
    - ``periodEnd`` (term/period end): past implies ENDED, future implies UNKNOWN
      (e.g. a parliamentary term may still be running but the person may have left)

    A death date in the past caps the status at ENDED: a deceased person is never
    emitted as a CURRENT or UNKNOWN office-holder.

    If the person should not be considered a PEP, return None.
    """
    if death_date is not None and ended_before(death_date, current_time - AFTER_DEATH):
        # If they died longer ago than AFTER_DEATH threshold, don't consider a PEP.
        return None

    if birth_date is not None and ended_before(birth_date, current_time - MAX_AGE):
        # If they're unrealistically old, assume they're not a PEP.
        return None

    # A death date entirely in the future is a data error and is ignored here.
    died = death_date is not None and not starts_after(death_date, current_time)

    # Determine effective start date (most specific first)
    effective_start_date = max(occupancy.get("startDate"), default=None)
    if effective_start_date is None:
        effective_start_date = max(occupancy.get("periodStart"), default=None)
    if effective_start_date is None:
        effective_start_date = max(occupancy.get("electionDate"), default=None)

    end_date = max(occupancy.get("endDate"), default=None)
    period_end = max(occupancy.get("periodEnd"), default=None)

    if not (
        death_date
        or birth_date
        or end_date
        or period_end
        or effective_start_date
        or no_end_implies_current
    ):
        # If we don't have any dates to work with, nor a really well-maintained source,
        # don't consider them a PEP.
        return None

    if categorisation is None:
        topics = position.get("topics")
    else:
        topics = categorisation.topics
    after_office = get_after_office(topics)

    # Individual end date is the most specific signal
    if end_date is not None:
        if ended_before(end_date, current_time):  # end_date is in the past
            if ended_before(end_date, current_time - after_office):
                # end_date is beyond after-office threshold
                return None
            else:
                # end_date is within after-office threshold
                return OccupancyStatus.ENDED
        elif (
            context.dataset.model.coverage
            and context.dataset.model.coverage.end
            and ended_before(context.dataset.model.coverage.end, current_time)
        ):  # end_date is in the future and dataset is beyond its coverage.
            # Don't trust future end dates beyond the known coverage date of the dataset
            context.log.warning(
                "Future Occupancy end date is beyond the dataset coverage date. "
                "Check if the source data is being updated.",
                person=person.id,
                position=position.id,
                end_date=end_date,
            )
            return OccupancyStatus.ENDED if died else OccupancyStatus.UNKNOWN
        else:  # end_date is in the future and coverage is unspecified or active
            return OccupancyStatus.ENDED if died else OccupancyStatus.CURRENT

    # Period end date: less specific — a future period end does not imply the person
    # is still in office. An MP could leave a term early
    if period_end is not None:
        if ended_before(period_end, current_time):  # period_end is in the past
            if ended_before(period_end, current_time - after_office):
                # period_end is beyond after-office threshold
                return None
            else:
                return OccupancyStatus.ENDED
    # If period_end is in the future, leave it to no_end_implies_current to determine status.

    # No end date of any kind
    dis_date = max(position.get("dissolutionDate"), default=None)
    # dissolution date is in the past:
    if dis_date is not None and ended_before(dis_date, current_time):
        if ended_before(dis_date, current_time - after_office):
            return None
        else:
            return OccupancyStatus.ENDED

    if effective_start_date is not None and ended_before(
        effective_start_date, current_time - MAX_OFFICE
    ):
        # start_date is older than MAX_OFFICE threshold - probably not a PEP
        return None

    if died:
        # A deceased person no longer holds the position, even if the source
        # hasn't recorded an end date.
        return OccupancyStatus.ENDED

    if no_end_implies_current:
        # This is for sources we are really confident will provide an end date or
        # totally remove the person soon enough after the person leaves the position
        return OccupancyStatus.CURRENT
    else:
        return OccupancyStatus.UNKNOWN
