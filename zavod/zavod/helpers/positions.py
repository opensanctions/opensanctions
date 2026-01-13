from datetime import datetime
from typing import Iterable, List, Optional

from banal import ensure_list
from followthemoney import registry

from zavod import helpers as h
from zavod import settings
from zavod.context import Context
from zavod.entity import Entity
from zavod.stateful.positions import (
    OccupancyStatus,
    PositionCategorisation,
    occupancy_status,
)


def make_position(
    context: Context,
    name: str,
    summary: Optional[str] = None,
    description: Optional[str] = None,
    country: Optional[str | Iterable[str]] = None,
    topics: Optional[List[str]] = None,
    subnational_area: Optional[str] = None,
    organization: Optional[Entity] = None,
    inception_date: Optional[Iterable[str]] = None,
    dissolution_date: Optional[Iterable[str]] = None,
    number_of_seats: Optional[str] = None,
    wikidata_id: Optional[str] = None,
    source_url: Optional[str] = None,
    lang: Optional[str] = None,
    id_hash_prefix: Optional[str] = None,
) -> Entity:
    """Creates a Position entity.

    Position categorisation should then be fetched using zavod.logic.pep.categorise
    and the result's is_pep checked.

    Args:
        context: The context to create the entity in.
        name: The name of the position.
        summary: A short summary of the position.
        description: A longer description of the position.
        country: The country or countries the position is in.
        subnational_area: The state or district the position is in.
        organization: The organization the position is a part of.
        inception_date: The date the position was created.
        dissolution_date: The date the position was dissolved.
        number_of_seats: The number of seats that can hold the position.
        wikidata_id: The Wikidata QID of the position.
        source_url: The URL of the source the position was found in.
        lang: The language of the position details.

    Returns:
        A new entity of type `Position`."""

    position = context.make("Position")

    parts: List[str] = [name]
    if country is not None:
        parts.extend(ensure_list(country))
    if inception_date is not None:
        parts.extend(ensure_list(inception_date))
    if dissolution_date is not None:
        parts.extend(ensure_list(dissolution_date))
    if subnational_area is not None:
        parts.extend(ensure_list(subnational_area))

    if wikidata_id is not None:
        position.id = wikidata_id
    else:
        position.id = context.make_id(*parts, hash_prefix=id_hash_prefix)

    position.add("name", name, lang=lang)
    position.add("summary", summary, lang=lang)
    position.add("description", description, lang=lang)
    position.add("country", country)
    position.add("topics", topics)
    position.add("organization", organization, lang=lang)
    position.add("subnationalArea", subnational_area, lang=lang)
    position.add("inceptionDate", inception_date)
    position.add("dissolutionDate", dissolution_date)
    position.add("numberOfSeats", number_of_seats)
    position.add("wikidataId", wikidata_id)
    position.add("sourceUrl", source_url)

    return position


def make_occupancy(
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
    status: Optional[OccupancyStatus] = None,
    propagate_country: bool = True,
) -> Optional[Entity]:
    """Creates and returns an Occupancy entity if the arguments meet our criteria
    for PEP position occupancy, otherwise returns None. Also adds the position countries
    and the `role.pep` topic to the person if an Occupancy is returned.
    **Emit the person after calling this to include these changes.**

    Unless `status` is overridden, Occupancies are only returned if end_date is None or
    less than the after-office period after current_time.

    current_time defaults to the process start date and time.

    The after-office threshold is determined based on the position topics.

    Occupancy.status is set to

    - `current` if `end_date` is `None` and `no_end_implies_current` is `True`,
      otherwise `status` will be `unknown`
    - `current` if `end_date` is some date in the future, unless the dataset
      `coverage.end` is a date in the past, in which case `status` will be `unknown`
    - `ended` if `end_date` is some date in the past.

    Args:
        context: The context to create the entity in.
        person: The person holding the position. They will be added to the
            `holder` property.
        position: The position held by the person. This will be added to the
            `post` property.
        no_end_implies_current: Set this to True if a dataset is regularly maintained
            and it can be assumed that no end date implies the person is currently
            occupying this position. In this case, `status` will be set to `current`.
            Otherwise, `status` will be set to `unknown`.
        current_time: Defaults to the run time of the current crawl.
        start_date: Set if the date the person started occupying the position is known.
        end_date: Set if the date the person left the position is known.
        status: Overrides determining PEP occupancy status
    """
    assert person.schema.is_a("Person")
    assert position.schema.is_a("Position")

    occupancy = context.make("Occupancy")
    # Include started and ended strings so that two occupancies, one missing start
    # and and one missing end, don't get normalisted to the same ID
    parts = [
        person.id,
        position.id,
        "started",
        start_date or "unknown",
        "ended",
        end_date or "unknown",
    ]
    occupancy.id = context.make_id(*parts)
    occupancy.add("holder", person)
    occupancy.add("post", position)

    h.apply_date(occupancy, "startDate", start_date)
    h.apply_date(occupancy, "endDate", end_date)

    if birth_date not in person.get("birthDate"):
        h.apply_date(person, "birthDate", birth_date)
    if death_date not in person.get("deathDate"):
        h.apply_date(person, "deathDate", death_date)

    if categorisation is not None and not categorisation.is_pep:
        context.log.warning(
            "Person is not categorized as a PEP, but was passed to make_occupancy",
            person=person.id,
            position=position.id,
            categorisation=categorisation,
        )
        return None

    if status is None:
        status = occupancy_status(
            context,
            person,
            position,
            no_end_implies_current,
            current_time,
            max(occupancy.get("startDate"), default=None),
            max(occupancy.get("endDate"), default=None),
            max(person.get("birthDate"), default=None),
            max(person.get("deathDate"), default=None),
            categorisation,
        )
    if status is None:
        return None

    if status != OccupancyStatus.UNKNOWN:
        occupancy.add("status", status.value)

    person.add("topics", "role.pep")
    if propagate_country:
        for country in position.get("country"):
            # Only propagate to Person.country it isn't already set
            # in another field (such as citizenship).
            if country not in person.get_type_values(registry.country, matchable=True):
                person.add("country", country)

    return occupancy
