from zavod import Context
from zavod import helpers as h
from zavod.stateful.positions import categorise
from zavod.util import Element


def crawl_member(context: Context, card: Element) -> None:
    link = h.xpath_element(card, './/h3//a[contains(@href, "/Members/")]')
    raw_name = h.element_text(link)
    name = h.strip_name_titles(context, raw_name)
    assert name is not None, raw_name
    clean_name = name.rstrip("/")

    person = context.make("Person")
    person.id = context.make_id(raw_name)
    person.add(
        "name",
        clean_name,
        lang="eng",
        original_value=raw_name if clean_name != raw_name else None,
    )
    person.add("sourceUrl", h.xpath_string(link, "./@href"))
    # Shura Council members must hold original Qatari nationality (Constitution of
    # Qatar, Article 80(1)). https://www.constituteproject.org/constitution/Qatar_2003
    person.add("citizenship", "qa")

    role = h.element_text(h.xpath_element(card, ".//h4"))
    res = context.lookup("position", role)
    assert res is not None, role
    for title in res.values:
        position = h.make_position(
            context,
            name=title,
            country="qa",
            topics=["gov.national", "gov.legislative"],
            # No Wikidata item covers either leadership office.
            wikidata_id="Q21328600" if "Member of the Shura Council" in title else None,
            lang="eng",
        )
        categorisation = categorise(context, position)
        if not categorisation.is_pep:
            continue
        occupancy = h.make_occupancy(
            context, person, position, categorisation=categorisation
        )
        if occupancy is not None:
            context.emit(position)
            context.emit(occupancy)
            context.emit(person)


def crawl(context: Context) -> None:
    doc = context.fetch_html(context.data_url, cache_days=1, absolute_links=True)
    cards = h.xpath_elements(doc, '//div[@class="content-block"]//div[@class="card"]')
    for card in cards:
        crawl_member(context, card)
