import re
from lxml.html import HtmlElement

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# The card subtitle reads "Member of Parliament for <constituency> Constituency".
CONSTITUENCY_RE = re.compile(
    r"^Member of Parliament for (?P<constituency>.+) Constituency$"
)


def parse_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    card: HtmlElement,
) -> None:

    raw_name = h.element_text(h.xpath_element(card, './/h5[@class="card-title"]'))
    subtitle = h.element_text(h.xpath_element(card, './/p[@class="card-text"]'))
    assert raw_name, "Empty member name"

    match = CONSTITUENCY_RE.match(subtitle)
    if match is None:
        raise ValueError(f"Unexpected member subtitle for {raw_name!r}: {subtitle!r}")
    constituency = match.group("constituency").strip()

    name = h.strip_name_titles(context, raw_name)
    if name is None:
        return

    person = context.make("Person")
    # Keyed on the name as published, not the title-stripped one: an ID must not depend
    # on the `names.prefixes_strip` config, or editing that list would mutate every ID
    # derived from it. See zavod/docs/best_practices/entity_id.md.
    person.id = context.make_id(raw_name, constituency)
    person.add("name", name, original_value=raw_name if name != raw_name else None)
    # A member of the National Parliament must be a citizen of Solomon Islands under
    # Chapter VI, Section 48(1)(a) of the Constitution of Solomon Islands.
    # https://www.constituteproject.org/constitution/Solomon_Islands_2018
    person.add("citizenship", "sb")

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
        name="Member of the National Parliament of Solomon Islands",
        country="sb",
        topics=["gov.national", "gov.legislative"],
        wikidata_id="Q17633943",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = context.fetch_html(context.data_url, cache_days=1)

    cards = h.xpath_elements(
        doc, '//*[contains(@class, "card")][.//h5[@class="card-title"]]'
    )
    for card in cards:
        parse_member(context, position, categorisation, card)
