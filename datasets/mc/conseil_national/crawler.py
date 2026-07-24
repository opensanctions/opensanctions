from typing import Any

from zavod import Context, Entity
from zavod import helpers as h
from zavod.stateful.positions import PositionCategorisation, categorise


def crawl_member(
    context: Context,
    page: dict[str, Any],
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    name = page["title"]["rendered"].strip()
    # Skip the section/index page; only individual councillors are members. The prefix
    # match still holds when the year in the title changes with each new legislature.
    if not name or name.lower().startswith("législature"):
        return

    person = context.make("Person")
    person.id = context.make_slug(page["slug"])
    person.add("name", name)
    person.add("sourceUrl", page.get("link"))
    # National Council members must have held Monegasque nationality for at least five
    # years (Constitution of Monaco, Article 54).
    # https://www.constituteproject.org/constitution/Monaco_2002
    person.add("citizenship", "mc")

    occupancy = h.make_occupancy(
        context, person, position, categorisation=categorisation
    )
    if occupancy is None:
        return
    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the National Council of Monaco",
        country="mc",
        wikidata_id="Q21328626",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    # Sitting councillors are the child pages of the stable "Les conseillers nationaux" page
    slug = "les-conseillers-nationaux"
    parents = context.fetch_json(context.data_url, params={"slug": slug}, cache_days=1)
    if len(parents) != 1:
        raise ValueError(
            f"Expected exactly one page for slug {slug!r}, got {len(parents)}"
        )

    pages: list[dict[str, Any]] = context.fetch_json(
        context.data_url,
        params={"parent": parents[0]["id"], "per_page": "100"},
        cache_days=1,
    )
    for page in pages:
        crawl_member(context, page, position, categorisation)
