import re

from lxml import html

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# The parliament site is served behind a CDN that rejects non-browser clients.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Cabinet members carry the honorific "Hon". Strip it so the emitted name is the person's
# actual name.
HONORIFIC_RE = re.compile(r"^Hon\.?\s+")


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    block: html.HtmlElement,
) -> None:
    constituency = h.element_text(h.xpath_element(block, './/p[@class="position"]'))
    name = HONORIFIC_RE.sub("", h.element_text(h.xpath_element(block, ".//h4"))).strip()
    assert name, "Empty member name"
    assert constituency, f"Empty constituency for {name!r}"

    person = context.make("Person")
    person.id = context.make_id(name, constituency)
    person.add("name", name)
    # Members must be a New Zealand citizen or a Permanent Resident of Niue (Constitution
    # of Niue, Article 17(1)(a)) — Niue is self-governing in free association with New
    # Zealand and has no separate citizenship. We therefore record country rather than
    # asserting a specific citizenship. https://faolex.fao.org/docs/pdf/niu132832.pdf
    person.add("country", "nu")

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    occupancy.add("constituency", constituency)

    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Niue Assembly",
        country="nu",
        wikidata_id="Q40011889",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = context.fetch_html(context.data_url, headers=HEADERS, cache_days=1)
    blocks = h.xpath_elements(
        doc,
        '//div[contains(@class, "cabinet-member")][.//p[@class="position"] and .//h4]',
    )
    if not blocks:
        raise ValueError("No member blocks found on the Niue government page")
    for block in blocks:
        crawl_member(context, position, categorisation, block)
