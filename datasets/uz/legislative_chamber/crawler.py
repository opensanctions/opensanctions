from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise


def crawl_deputy(
    context: Context,
    row: dict[str, Any],
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    slug = row.pop("slug")
    person = context.make("Person")
    person.id = context.make_slug("deputy", str(row.pop("id")))
    h.apply_name(
        person,
        first_name=row.pop("first_name"),
        patronymic=row.pop("middle_name"),
        last_name=row.pop("last_name"),
        lang="eng",
    )

    detail = context.fetch_json(
        f"{context.data_url.replace('list/', 'detail/')}{slug}/", cache_days=7
    )
    h.apply_date(person, "birthDate", detail.pop("birth_date"))
    person.add("birthPlace", detail.pop("birth_place"), lang="uzb")
    person.add("email", detail.pop("email"))
    # Deputies of the Legislative Chamber must be citizens of Uzbekistan (Constitution
    # art. 77). https://www.constituteproject.org/constitution/Uzbekistan_2011
    person.add("citizenship", "uz")

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return

    okrug = row.pop("okrug")
    fraction = row.pop("fraction")
    # Single-mandate deputies carry an electoral district (okrug); party-list do not.
    if okrug is not None:
        occupancy.add("constituency", okrug.pop("title"), lang="uzb")
    if fraction is not None:
        occupancy.add("politicalGroup", fraction.pop("title"), lang="uzb")
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Legislative Chamber of the Oliy Majlis of Uzbekistan",
        country="uz",
        topics=["gov.national", "gov.legislative"],
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    deputies = context.fetch_json(context.data_url, cache_days=1)
    for row in deputies:
        crawl_deputy(context, row, position, categorisation)
