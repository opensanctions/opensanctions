from banal import ensure_list
from typing import Optional, Iterable, List
from datetime import datetime

from zavod.context import Context
from zavod.entity import Entity
from zavod import settings
from zavod.logic.pep import occupancy_status, OccupancyStatus
from zavod.settings import API_URL, API_KEY


class Annotation:
    def __init__(
        self, names: List[str], countries: List[str], topics: List[str], is_pep: bool
    ):
        self.names = names
        self.countries = countries
        self.topics = topics
        self.is_pep = is_pep


def check_position(
    context: Context,
    position_id: str,
    names: List[str],
    countries: List[str],
    topics: List[str],
    is_pep: bool,
) -> Annotation:
    if API_URL is None or API_KEY is None:
        return None
    url = f"{API_URL}/positions/{position_id}"
    headers = {"authorization": API_KEY}
    res = context.http.get(url, headers=headers)

    if res.status_code == 200:
        data = res.json()
    elif res.status_code == 404:
        url = f"{API_URL}/positions/"
        body = {
            "entity_id": position_id,
            "names": names,
            "countries": countries,
            "topics": topics,
            "is_pep": is_pep,
        }
        res = context.http.post(url, headers=headers, json=body)
        res.raise_for_status()
        data = res.json()
    else:
        res.raise_for_status()
      
    return Annotation(
        data["names"],
        data["countries"],
        data["topics"],
        data["is_pep"],
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
    is_pep: Optional[bool] = True,
) -> Optional[Entity]:
    """Creates and returns a Position entity if is_pep is True or if it's
    confirmed to be a PEP position in the positions database. Otherwise returns
    None.

    Also ensures the position exists in the positions database for further
    categorisation.

    Topics in the positions database override topics supplied to this helper.

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
        is_pep: Whether the position is known to be a PEP.

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

    annotation = check_position(context, position.id, [name], ensure_list(country), ensure_list(topics), is_pep)
    if annotation is None:
        context.log.warning("PEP database unavailable. Configure API_URL and API_KEY")
        return
    if annotation.is_pep is None:
        context.log.info(f"Position {country} {name} not yet categorised as PEP or not.")
        return
    if not annotation.is_pep:
        context.log.info(f"Position {country} {name} is not a PEP position.")
        return

    position.add("name", name, lang=lang)
    position.add("summary", summary, lang=lang)
    position.add("description", description, lang=lang)
    position.add("country", country)
    position.add("topics", annotation.topics)
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
    status: Optional[OccupancyStatus] = None,
) -> Optional[Entity]:
    """Creates and returns an Occupancy entity if the arguments meet our criteria
    for PEP position occupancy, otherwise returns None. Also adds the position countries
    and the `role.pep` topic to the person if an Occupancy is returned.

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
    if status is None:
        status = occupancy_status(
            context,
            person,
            position,
            no_end_implies_current,
            current_time,
            start_date,
            end_date,
            birth_date,
            death_date,
        )
    if status is None:
        return None

    occupancy = context.make("Occupancy")
    # Include started and ended strings so that two occupancies, one missing start
    # and and one missing end, don't get normalisted to the same ID
    parts = [person.id, position.id, "started", start_date, "ended", end_date]
    occupancy.id = context.make_id(*parts)
    occupancy.add("holder", person)
    occupancy.add("post", position)
    occupancy.add("startDate", start_date)
    occupancy.add("endDate", end_date)
    occupancy.add("status", status.value)

    person.add("topics", "role.pep")
    person.add("country", position.get("country"))

    return occupancy
