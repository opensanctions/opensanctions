from lxml.etree import _Element as Element

from zavod import Context
from zavod import helpers as h
from zavod.extract import zyte_api
from zavod.stateful.positions import categorise

MEMBER = "Member of the Consultative Assembly of Saudi Arabia"
# Only the member seat has a Wikidata item.
POSITION_QIDS = {MEMBER: "Q21328601"}
CARD = "contains(concat(' ', normalize-space(@class), ' '), ' council-member-card ')"


def crawl_person(
    context: Context, seen: dict[str, str], link: Element, titles: list[str]
) -> None:
    href = link.get("href")
    assert href is not None, link
    slug = href.rstrip("/").rsplit("/", 1)[-1]

    person = context.make("Person")
    person.id = context.make_id(slug)
    override = context.lookup("card_name", slug)
    if override is not None:
        person.add("name", override.name, lang=override.lang)
    else:
        raw_name = h.element_text(link)
        name = h.strip_name_titles(context, raw_name)
        assert name is not None, href
        # The source has captioned one card with another member's name.
        if name in seen and seen[name] != href:
            context.log.warning(
                "Two cards share a name", name=name, urls=[seen[name], href]
            )
        seen[name] = href
        original = raw_name if name != raw_name else None
        person.add("name", name, original_value=original)
    person.add("sourceUrl", href)
    person.add("citizenship", "sa")

    for title in titles:
        position = h.make_position(
            context,
            name=title,
            country="sa",
            topics=["gov.national", "gov.legislative"],
            wikidata_id=POSITION_QIDS.get(title),
            lang="eng",
        )
        categorisation = categorise(context, position)
        if not categorisation.is_pep:
            continue
        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation
        )
        if occupancy is not None:
            context.emit(occupancy)
            context.emit(position)
            context.emit(person)


def crawl(context: Context) -> None:
    seen: dict[str, str] = {}
    visited: set[str] = set()
    url: str | None = context.data_url
    while url is not None:
        visited.add(url)
        doc = zyte_api.fetch_html(
            context,
            url,
            unblock_validator=f".//a[{CARD}]",
            html_source="httpResponseBody",
            geolocation="SA",
            absolute_links=True,
        )
        # Leadership cards: the name links to the CV, the role label sits below
        # it. The <p> bio under the role ("Speaker since 1430 AH, ...") is free text.
        for card in h.xpath_elements(doc, f".//div[{CARD}]"):
            role = h.element_text(h.xpath_element(card, ".//h6"))
            res = context.lookup("position", role)
            assert res is not None, role
            crawl_person(context, seen, h.xpath_element(card, ".//h5/a"), res.values)

        # Member cards are links to the CV, captioned with the member's name.
        for card in h.xpath_elements(doc, f".//a[{CARD}]"):
            crawl_person(context, seen, card, [MEMBER])

        next_urls = h.xpath_strings(doc, ".//a[normalize-space()='>']/@href")
        assert len(next_urls) <= 1, next_urls
        url = next_urls[0] if next_urls and next_urls[0] not in visited else None
