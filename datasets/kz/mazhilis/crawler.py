from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise


def crawl_deputy(
    context: Context,
    row: dict[str, Any],
    ru: dict[str, Any] | None,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    person = context.make("Person")
    person.id = context.make_slug("deputy", str(row.pop("id")))
    # Default names are Kazakh Cyrillic; the Russian-Cyrillic forms differ in
    # transliteration (e.g. Қарақат / Каракат) and are kept as aliases.
    h.apply_name(
        person,
        first_name=row.pop("first_name"),
        patronymic=row.pop("patronymic"),
        last_name=row.pop("last_name"),
        lang="kaz",
    )
    if ru is not None:
        h.apply_name(
            person,
            first_name=ru.get("first_name"),
            patronymic=ru.get("patronymic"),
            last_name=ru.get("last_name"),
            lang="rus",
        )
    email = row.pop("email")
    if email is not None:
        person.add("email", email.replace(" ", ""))
    # Deputies of the Mäjilis must be citizens of Kazakhstan (Constitution art. 51(4)).
    # https://www.constituteproject.org/constitution/Kazakhstan_2017
    person.add("citizenship", "kz")

    region = row.pop("region")
    party = row.pop("party")
    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    # Single-mandate deputies carry an electoral region; party-list deputies do not.
    if region is not None:
        occupancy.add("constituency", region.get("name"), lang="kaz")
        if ru is not None and ru.get("region") is not None:
            occupancy.add("constituency", ru["region"].get("name"), lang="rus")
    if party is not None:
        occupancy.add("politicalGroup", party.get("name"), lang="kaz")
        if ru is not None and ru.get("party") is not None:
            occupancy.add("politicalGroup", ru["party"].get("name"), lang="rus")

    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Mäjilis of Kazakhstan",
        country="kz",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21328574",
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    deputies = context.fetch_json(
        context.data_url, params={"ord": "last_name"}, cache_days=1
    )
    # The Russian variant lives at the same URL behind Accept-Language; zavod's cache
    # keys on URL only, so fetch it uncached to avoid colliding with the Kazakh response.
    ru_rows = context.fetch_json(
        context.data_url, params={"ord": "last_name"}, headers={"Accept-Language": "ru"}
    )
    ru = {r["id"]: r for r in ru_rows}

    for row in deputies:
        crawl_deputy(context, row, ru.get(row["id"]), position, categorisation)
