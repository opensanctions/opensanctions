from typing import cast

from lxml import html

from zavod import Context, Entity
from zavod import helpers as h
from zavod.stateful.positions import PositionCategorisation, categorise


def crawl_member(
    context: Context,
    slug: str,
    name: str,
    source_url: str,
    position: Entity,
    categorisation: PositionCategorisation,
) -> None:
    person = context.make("Person")
    person.id = context.make_slug(slug)
    person.add("name", name, lang="eng")
    person.add("sourceUrl", source_url)
    # Members of the Council of the Republic must be citizens of Belarus (Constitution
    # of the Republic of Belarus, Article 92).
    # https://www.constituteproject.org/constitution/Belarus_2004
    person.add("citizenship", "by")

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
        name="Member of the Council of the Republic of Belarus",
        country="by",
        wikidata_id="Q15623433",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = context.fetch_html(context.data_url, cache_days=1)
    cast(html.HtmlElement, doc).make_links_absolute(context.data_url)
    seen: set[str] = set()
    for link in h.xpath_elements(doc, '//a[contains(@href, "/senators-en/view/")]'):
        href = link.get("href")
        assert href is not None
        slug = href.rstrip("/").split("/")[-1]
        name = h.element_text(link)
        if not name or slug in seen:
            continue
        seen.add(slug)
        crawl_member(context, slug, name, href, position, categorisation)

    if not seen:
        raise ValueError("No senators found in the Council directory")
