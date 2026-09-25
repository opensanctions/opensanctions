import re
from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import categorise

TOPICS = ["gov.national", "gov.legislative"]
PAGE_SIZE = 200


def crawl_deputy_position(
    context: Context,
    person: Entity,
    dep: dict[str, Any],
    is_current: bool,
) -> None:
    leadership_position_name = dep.pop("deputyPosition_name")

    if "President" in leadership_position_name:
        position = h.make_position(
            context,
            name="President of the National Assembly of Armenia",
            country="am",
            topics=TOPICS,
            wikidata_id="Q30340137",
            lang="eng",
        )
    elif "Deputy Speaker" in leadership_position_name:
        position = h.make_position(
            context,
            name="Deputy Speaker of the National Assembly of Armenia",
            country="am",
            topics=TOPICS,
            lang="eng",
        )
    else:
        raise ValueError(
            f"Unrecognized National Assembly leadership position: {leadership_position_name!r}"
        )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        start_date=dep.pop("start_time"),
        end_date=dep.pop("end_time"),
        no_end_implies_current=is_current,
    )
    if occupancy is None:
        return
    context.emit(occupancy)
    context.emit(person)


def crawl_member(
    context: Context,
    full_url: str,
    member_id: int,
) -> None:
    record = context.fetch_json(f"{full_url}/{member_id}", cache_days=1)[
        "parliamentMember"
    ]

    first_name = record.pop("first_name")
    last_name = record.pop("last_name")
    birthdate = record.pop("birthdate")

    person = context.make("Person")
    person.id = context.make_id(first_name, last_name, birthdate)
    h.apply_name(
        person,
        first_name=first_name,
        patronymic=record.pop("patronymic", None),
        last_name=last_name,
        lang="eng",
    )
    h.apply_date(person, "birthDate", birthdate)
    # RA Constitution Art. 48 requires exclusive Armenian citizenship, held for the
    # preceding four years, to be elected a deputy of the National Assembly.
    # http://www.parliament.am/legislation.php?sel=show&ID=1&lang=eng
    person.add("citizenship", "am")
    person.add("email", record.pop("email"))

    is_current = record.pop("isConvocationCurrent")

    constituency_description = record.pop("constituencie_description")
    if constituency_description is not None:
        # Territorial constituencies list multiple districts separated by raw
        # "<br>" tags, e.g. "Yerevan <br>\nAvan, Nor Nork, ... districts".
        constituency = re.sub(r"\s*<br\s*/?>\s*", ", ", constituency_description)
    else:
        constituency = record.pop("constituencie_serial_number")
    for party in record.pop("parties"):
        person.add("political", party.pop("party_name"))

    political_groups: set[str] = set()
    start_dates: list[str] = []
    end_dates: list[str] = []
    for faction_position in record.pop("factionPositions"):
        political_groups.add(faction_position.pop("faction_name"))
        faction_start = faction_position.pop("start_time")
        faction_end = faction_position.pop("end_time")
        if faction_start is not None:
            start_dates.append(faction_start)
        if faction_end is not None:
            end_dates.append(faction_end)

    member_start_date = min(start_dates) if start_dates else None
    member_end_date = max(end_dates) if end_dates else None

    deputy_positions = record.pop("deputyPositions")

    position = h.make_position(
        context,
        name="Member of the National Assembly of Armenia",
        country="am",
        topics=TOPICS,
        wikidata_id="Q17277248",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return

    context.emit(position)
    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        start_date=member_start_date,
        end_date=member_end_date,
        no_end_implies_current=is_current,
    )
    if occupancy is not None:
        occupancy.add("constituency", constituency)
        for group in political_groups:
            occupancy.add("politicalGroup", group)
        context.emit(occupancy)
        context.emit(person)

    for dep in deputy_positions:
        crawl_deputy_position(context, person, dep, is_current)


def crawl_convocation(
    context: Context,
    convocation_id: int,
    convocation_url: str,
    full_url: str,
) -> None:
    """Collect a convocation's member ids (from the paginated listing +
    the separate leadership field), which key the per-member endpoint
    used to crawl each member's full record."""
    page = context.fetch_json(
        convocation_url,
        params={"limit": PAGE_SIZE, "offset": 0, "convocationId": convocation_id},
        cache_days=1,
    )
    # Leadership is only read from the first page, and the letter-grouped
    # member listing excludes it; collect its ids separately.
    leadership = page["parliamentMembersDeputyPosition"]
    member_ids = {member["id"] for member in leadership}

    # the API response is a dict whose keys are letters of the alphabet,
    # grouping members by the first letter of their surnames;
    # an empty mapping signals the end of pagination
    offset = 0
    while letters := page["parliamentMembers"]:
        for members in letters.values():
            member_ids.update(member["id"] for member in members)
        offset += PAGE_SIZE
        page = context.fetch_json(
            convocation_url,
            params={
                "limit": PAGE_SIZE,
                "offset": offset,
                "convocationId": convocation_id,
            },
            cache_days=1,
        )

    for member_id in sorted(member_ids):
        crawl_member(
            context,
            full_url,
            member_id,
        )


def crawl(context: Context) -> None:
    convocation_url = context.data_url.replace("convocations", "parliamentMembers")
    full_url = convocation_url.replace("/all/en", "/full/en")

    convocations = context.fetch_json(context.data_url, cache_days=1)
    for convocation in convocations:
        convocation_id = convocation.pop("id")

        crawl_convocation(
            context,
            convocation_id,
            convocation_url,
            full_url,
        )
