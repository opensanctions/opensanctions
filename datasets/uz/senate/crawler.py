from typing import Any

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# Per-senator birth details live on the detail endpoint. The list serves names in the
# locale chosen by Accept-Language (default Uzbek Latin); the Russian-Cyrillic variant
# is kept as an alias.
DETAIL_URL = "https://senat.uz/api/v1/site/person/"

IGNORE = [
    "avatar",
    "work_place",  # free-text role/job, varies (committee role, regional governor, ...)
    "committee",  # committee membership — no entity field
    "reception_days",
    "reception_place",
]


def crawl_senator(
    context: Context,
    row: dict[str, Any],
    ru: dict[str, Any] | None,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    senator_id = row.pop("id")
    person = context.make("Person")
    person.id = context.make_slug("senator", str(senator_id))
    h.apply_name(person, full=row.pop("full_name"), lang="uzb")
    if ru is not None:
        h.apply_name(person, full=ru.get("full_name"), lang="rus", alias=True)

    detail = context.fetch_json(f"{DETAIL_URL}{senator_id}", cache_days=7)
    detail = detail.get("data", detail)
    h.apply_date(person, "birthDate", detail.get("birth_date"))
    person.add("birthPlace", detail.get("birth_region"), lang="uzb")
    person.add("birthPlace", detail.get("birth_district"), lang="uzb")
    person.add("email", detail.get("email"))
    # Members of the Senate must be citizens of Uzbekistan (Constitution art. 77).
    # https://www.constituteproject.org/constitution/Uzbekistan_2011
    person.add("citizenship", "uz")

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    context.emit(occupancy)
    context.emit(person)
    context.audit_data(row, ignore=IGNORE)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Senate of the Oliy Majlis of Uzbekistan",
        country="uz",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q21295154",
        lang="eng",
    )
    categorisation = categorise(context, position)
    context.emit(position)

    senators = context.fetch_json(context.data_url, cache_days=1)["data"]["results"]
    # Russian variant sits at the same URL behind Accept-Language; zavod's cache keys on
    # URL only, so fetch it uncached to avoid colliding with the default response.
    ru = {
        r["id"]: r
        for r in context.fetch_json(
            context.data_url, headers={"Accept-Language": "ru"}
        )["data"]["results"]
    }

    for row in senators:
        crawl_senator(context, row, ru.get(row["id"]), position, categorisation)
