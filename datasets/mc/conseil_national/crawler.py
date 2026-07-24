from typing import Any

from zavod import Context, Entity
from zavod import helpers as h
from zavod.stateful.positions import PositionCategorisation, categorise

# The current legislature's member pages are children of this WordPress page. The children
# also include the "Législature 2023-2028" section page itself, which is not a member.
CURRENT_LEGISLATURE_PARENT = 13076


def crawl_member(
    context: Context,
    page: dict[str, Any],
    position: Entity,
    categorisation: PositionCategorisation,
) -> bool:
    name = page["title"]["rendered"].strip()
    # Skip the section/index page; only individual councillors are members. The prefix
    # match still holds when the year in the title changes with each new legislature.
    if not name or name.lower().startswith("législature"):
        return False

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
        return False
    context.emit(occupancy)
    context.emit(person)
    return True


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the National Council of Monaco",
        country="mc",
        wikidata_id="Q21328626",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    pages: list[dict[str, Any]] = context.fetch_json(
        context.data_url,
        params={"parent": CURRENT_LEGISLATURE_PARENT, "per_page": "100"},
        cache_days=1,
    )
    count = 0
    for page in pages:
        if crawl_member(context, page, position, categorisation):
            count += 1

    if count == 0:
        raise ValueError("No National Council members found")
