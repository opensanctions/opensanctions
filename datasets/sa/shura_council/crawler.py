from lxml.etree import _Element as Element

from zavod import Context
from zavod import helpers as h
from zavod.extract import zyte_api
from zavod.stateful.positions import categorise

MEMBER = "Member of the Consultative Assembly of Saudi Arabia"
CARD = "contains(concat(' ', normalize-space(@class), ' '), ' council-member-card ')"


def crawl_person(
    context: Context, seen: dict[str, str], link: Element, titles: list[str]
) -> None:
    href = link.get("href")
    assert href is not None, link
    slug = href.rstrip("/").rsplit("/", 1)[-1]

    raw_name = h.element_text(link)

    person = context.make("Person")
    override = context.lookup("card_name", slug)
    if override is not None:
        # The caption names another member, so key on the corrected name.
        person.id = context.make_id(override.name)
        person.add("name", override.name)
        person.add("name", override.arabic_name, lang="ara")
    else:
        person.id = context.make_id(raw_name)
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
            # Only the member seat has a Wikidata item.
            wikidata_id="Q21328601" if title == MEMBER else None,
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
    # The roster is paged 9 cards at a time; a larger page size returns all of it.
    url = f"{context.data_url}?WCM_PageSize.64c7eaaf-7977-40f9-8593-3711eb70462a=500"
    doc = zyte_api.fetch_html(
        context,
        url,
        unblock_validator=f".//a[{CARD}]",
        html_source="httpResponseBody",
        geolocation="SA",
        absolute_links=True,
    )
    pager = h.xpath_strings(doc, ".//a[contains(@href, 'WCM_Page.')]/@href")
    assert len(pager) == 0, f"Roster is paginated, check the page size: {pager}"

    seen: dict[str, str] = {}
    # Leadership cards: the name links to the CV, the role label sits below it.
    # The <p> bio under the role ("Speaker since 1430 AH, ...") is free text.
    for card in h.xpath_elements(doc, f".//div[{CARD}]"):
        role = h.element_text(h.xpath_element(card, ".//h6"))
        res = context.lookup("position", role)
        assert res is not None, role
        crawl_person(context, seen, h.xpath_element(card, ".//h5/a"), res.values)

    # Member cards are links to the CV, captioned with the member's name.
    for card in h.xpath_elements(doc, f".//a[{CARD}]"):
        crawl_person(context, seen, card, [MEMBER])
