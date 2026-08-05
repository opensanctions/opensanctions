import re
from typing import Any

from lxml import html

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# The members page slug changes with every Congress (22nd-congress, 23rd-congress,
# 24th-congress-members, ...), so the crawler discovers the current page rather than
# hardcoding a slug. We search the WordPress REST API for pages whose title matches
# "<N>th Congress Members" and take the one with the highest ordinal.
TITLE_RE = re.compile(r"(\d+)(?:st|nd|rd|th)\s+Congress\s+Members\s*$", re.IGNORECASE)


def find_current_page(context: Context) -> dict[str, Any]:
    pages = context.fetch_json(
        context.data_url,
        params={"search": "Congress Members", "per_page": "100"},
        cache_days=1,
    )
    current: dict[str, Any] | None = None
    highest = -1
    for page in pages:
        title = page["title"]["rendered"].strip()
        match = TITLE_RE.match(title)
        if match is None:
            continue
        ordinal = int(match.group(1))
        if ordinal > highest:
            highest = ordinal
            current = page
    if current is None:
        raise ValueError("Could not find a current Congress Members page")
    return current


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    box: html.HtmlElement,
) -> None:
    title_el = h.xpath_element(
        box, './/*[contains(@class, "elementor-image-box-title")]'
    )
    raw_name = h.element_text(title_el)
    name = h.strip_name_titles(context, raw_name)
    assert name, "Empty member name"

    # Either the state the member represents ("State of Chuuk") or, for the three
    # chamber officers, their role in Congress ("SPEAKER").
    description = h.element_text(
        h.xpath_element(
            box, './/*[contains(@class, "elementor-image-box-description")]'
        )
    )

    person = context.make("Person")
    person.id = context.make_id(name)
    person.add(
        "name",
        name,
        lang="eng",
        original_value=raw_name if name != raw_name else None,
    )
    # A short biography PDF is linked from the member's name where available.
    source_urls = h.xpath_strings(title_el, ".//a/@href")
    person.add("sourceUrl", source_urls[0] if source_urls else None)
    # No person may be elected to Congress unless they have been a citizen of the FSM for
    # at least fifteen years (FSM Constitution, Article IX, Section 9).
    # https://www.fsmlaw.org/fsm/constitution/article9.htm
    person.add("citizenship", "fm")

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    if description.startswith("State of "):
        occupancy.add("constituency", description)
    else:
        occupancy.add("description", description)

    context.emit(occupancy)
    context.emit(person)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the Congress of the Federated States of Micronesia",
        country="fm",
        wikidata_id="Q21328596",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    page = find_current_page(context)
    doc = html.fromstring(page["content"]["rendered"])
    boxes = h.xpath_elements(
        doc, '//div[contains(@class, "elementor-image-box-content")]'
    )
    if not boxes:
        raise ValueError("No member widgets found on the Congress Members page")
    for box in boxes:
        crawl_member(context, position, categorisation, box)
