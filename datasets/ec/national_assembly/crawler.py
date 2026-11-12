from typing import Any
from urllib.parse import urljoin

from zavod.stateful.positions import (
    OccupancyStatus,
    PositionCategorisation,
    categorise,
)

from zavod import Context, Entity
from zavod import helpers as h

TOPICS = ["gov.national", "gov.legislative"]


def fetch_members(
    context: Context, term_id: int, id_type: int, only_active: bool
) -> list[dict[str, Any]]:
    url = urljoin(context.data_url, "assemblyman/assembly")
    params = {
        "idType": id_type,
        "onlyActive": "true" if only_active else "false",
        "idPeriod": term_id,
        # Required: without it the API returns only a handful of members.
        "idTerritorial": "",
    }
    members: list[dict[str, Any]] = context.fetch_json(url, params=params)
    # The API answers an unknown or ignored parameter with an empty list, not an error.
    assert len(members) > 0, params
    return members


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    term: dict[str, Any],
    row: dict[str, Any],
    period_end: str | None,
    alternate: bool,
    status: OccupancyStatus | None,
) -> None:
    member_id = row.pop("id")
    first_name = row.pop("firstName")
    last_name = row.pop("lastname")
    # A disabled account ("disabled user") listed among the 2023-2025 principals.
    if (first_name, last_name) == ("INHABILITADO", "USUARIO"):
        context.log.info("Skipping placeholder account", id=member_id)
        return
    label = row.pop("description")
    expected = term["name"].removeprefix("PERÍODO ")
    if label != expected:
        raise ValueError(f"Term label {label!r} does not match period {expected!r}")

    person = context.make("Person")
    person.id = context.make_id(member_id)
    h.apply_name(person, first_name=first_name, last_name=last_name)
    # Members of the National Assembly must be Ecuadorian nationals: Constitution of
    # Ecuador (2008), Art. 119. https://pdba.georgetown.edu/Constitutions/Ecuador/ecuador08.html
    person.add("citizenship", "ec")

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
        period_start=term["startDate"][:10],
        period_end=period_end,
        no_end_implies_current=not alternate,
        status=status,
    )
    if occupancy is None:
        return
    occupancy.add("constituency", row.pop("territorial"))
    if alternate:
        occupancy.add("description", "Alternate (suplente)", lang="eng")
    context.emit(occupancy)
    context.emit(person)


def crawl_term(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    term: dict[str, Any],
) -> None:
    # HAL resources carry their id only in the self link, which points at an internal
    # host (ecurulservices.pro:3334), e.g. http://ecurulservices.pro:3334/period/8.
    term_id = int(term["_links"]["self"]["href"].rsplit("/", 1)[1])
    is_sitting = {"ACTIVE": True, "CLOSED": False}[term["periodStatus"]]
    # The sitting term's endDate is its scheduled end; a closed term's is its real end,
    # e.g. the dissolution of the 2021-2023 Assembly on 2023-05-17.
    period_end = None if is_sitting else term["endDate"][:10]

    active_ids: set[int] = set()
    if is_sitting:
        active = fetch_members(context, term_id, id_type=1, only_active=True)
        active_ids = {row["id"] for row in active}
        if len(active_ids) != term["legislatorsNumber"]:
            context.log.warning(
                "Active principals do not match the seats",
                active=len(active_ids),
                seats=term["legislatorsNumber"],
            )

    # idType 1: principal members; idType 2: alternates (suplentes).
    for id_type, alternate in ((1, False), (2, True)):
        for row in fetch_members(context, term_id, id_type=id_type, only_active=False):
            # A principal of the sitting term the source no longer lists as active has
            # left mid-term; the source gives no date for it. Alternates are flagged
            # active too, but that can't mean sitting: active principals fill every seat.
            status = None
            if is_sitting and not alternate and row["id"] not in active_ids:
                status = OccupancyStatus.ENDED
            crawl_member(
                context,
                position,
                categorisation,
                term,
                row,
                period_end=period_end,
                alternate=alternate,
                status=status,
            )


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the National Assembly of Ecuador",
        wikidata_id="Q21295982",
        country="ec",
        topics=TOPICS,
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        context.log.warning("Position is not categorised as PEP", position=position.id)
        return
    context.emit(position)

    data = context.fetch_json(urljoin(context.data_url, "period"))
    terms = data["_embedded"]["periods"]
    statuses = [term["periodStatus"] for term in terms]
    if statuses.count("ACTIVE") != 1:
        context.log.warning("Expected exactly one sitting term", statuses=statuses)

    cutoff = h.earliest_term_start(TOPICS)
    # Newest first, so the first out-of-window term ends the loop.
    for term in sorted(terms, key=lambda t: t["startDate"], reverse=True):
        if term["endDate"][:10] < cutoff:
            context.log.info("Term predates the PEP window", term=term["name"])
            break
        # The 2007-2008 Constituent Assembly and the 2008-2009 Legislative Commission
        # preceded the National Assembly.
        if not term["name"].startswith("PERÍODO "):
            context.log.info("Not a National Assembly term", term=term["name"])
            continue
        crawl_term(context, position, categorisation, term)
