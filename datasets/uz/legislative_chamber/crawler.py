from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# The list endpoint serves names in the locale chosen by Accept-Language (default is
# Uzbek Latin); we fetch the Cyrillic (oz) and Russian (ru) variants too and merge by
# id. Per-deputy birth details live on the detail endpoint.
DETAIL_URL = "https://parliament.gov.uz/api/v1/structure/deputy/detail/"

IGNORE = [
    "first_name",  # consumed via .get() (also read from the oz/ru locale rows)
    "last_name",
    "middle_name",
    "image",
    "work_place",  # free-text prior workplace
    "deputy_assistant",
    "deputy_assistant_phone",
    "committee",  # committee membership — no entity field
]


def apply_names(person: Entity, row: dict[str, Any], lang: str, alias: bool) -> None:
    h.apply_name(
        person,
        first_name=row.get("first_name"),
        patronymic=row.get("middle_name"),
        last_name=row.get("last_name"),
        lang=lang,
        alias=alias,
    )


def crawl_deputy(
    context: Context,
    row: dict[str, Any],
    oz: dict[str, Any] | None,
    ru: dict[str, Any] | None,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    slug = row.pop("slug")
    person = context.make("Person")
    person.id = context.make_slug("deputy", str(row.pop("id")))
    apply_names(person, row, lang="uzb", alias=False)  # Uzbek Latin (default)
    if oz is not None:
        apply_names(person, oz, lang="uzb", alias=True)  # Uzbek Cyrillic
    if ru is not None:
        apply_names(person, ru, lang="rus", alias=True)  # Russian

    detail = context.fetch_json(f"{DETAIL_URL}{slug}/", cache_days=7)
    h.apply_date(person, "birthDate", detail.get("birth_date"))
    person.add("birthPlace", detail.get("birth_place"), lang="uzb")
    # Deputies of the Legislative Chamber must be citizens of Uzbekistan (Constitution
    # art. 77). https://www.constituteproject.org/constitution/Uzbekistan_2011
    person.add("citizenship", "uz")

    okrug = row.pop("okrug")
    fraction = row.pop("fraction")
    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    # Single-mandate deputies carry an electoral district (okrug); party-list do not.
    if okrug is not None:
        occupancy.add("constituency", okrug.get("title"), lang="uzb")
    if fraction is not None:
        occupancy.add("politicalGroup", fraction.get("title"), lang="uzb")
        if ru is not None and ru.get("fraction") is not None:
            occupancy.add("politicalGroup", ru["fraction"].get("title"), lang="rus")

    context.emit(occupancy)
    context.emit(person)
    context.audit_data(row, ignore=IGNORE)


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
    # Other locales sit at the same URL behind Accept-Language; zavod's cache keys on
    # URL only, so fetch them uncached to avoid colliding with the default response.
    oz = {
        r["id"]: r
        for r in context.fetch_json(context.data_url, headers={"Accept-Language": "oz"})
    }
    ru = {
        r["id"]: r
        for r in context.fetch_json(context.data_url, headers={"Accept-Language": "ru"})
    }

    for row in deputies:
        crawl_deputy(
            context, row, oz.get(row["id"]), ru.get(row["id"]), position, categorisation
        )
