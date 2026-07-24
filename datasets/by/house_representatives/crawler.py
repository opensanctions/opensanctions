from typing import cast

from lxml import html

from zavod import Context, Entity
from zavod import helpers as h
from zavod.extract import zyte_api
from zavod.stateful.positions import PositionCategorisation, categorise

# house.gov.by is geo-restricted (times out from non-regional egress), so it is fetched
# through the Zyte API. A Polish exit is the closest well-supported EU location.
GEOLOCATION = "pl"
UNBLOCK_VALIDATOR = './/a[contains(@href, "/deputies-en/view/")]'


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
    # Members of the House of Representatives must be citizens of Belarus (Constitution
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
        name="Member of the House of Representatives of Belarus",
        country="by",
        wikidata_id="Q14335901",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = zyte_api.fetch_html(
        context,
        context.data_url,
        unblock_validator=UNBLOCK_VALIDATOR,
        geolocation=GEOLOCATION,
        cache_days=1,
    )
    cast(html.HtmlElement, doc).make_links_absolute(context.data_url)
    seen: set[str] = set()
    for link in h.xpath_elements(doc, UNBLOCK_VALIDATOR):
        href = link.get("href")
        assert href is not None
        slug = href.rstrip("/").split("/")[-1]
        name = h.element_text(link)
        if not name or slug in seen:
            continue
        seen.add(slug)
        crawl_member(context, slug, name, href, position, categorisation)

    if not seen:
        raise ValueError("No deputies found in the House directory")
