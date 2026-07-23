import re

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.stateful.positions import PositionCategorisation, categorise

# The parliament site returns HTTP 403 to the default client; it serves the page to a
# standard browser User-Agent.
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}

# Member names carry the honorific "Hon.". Strip it so the emitted name is the person's
# actual name.
HONORIFIC_RE = re.compile(r"^Hon\.\s*", re.IGNORECASE)

# The card subtitle reads "Member of Parliament for <constituency> Constituency".
CONSTITUENCY_RE = re.compile(
    r"^Member of Parliament for (?P<constituency>.+) Constituency$"
)


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    name: str,
    subtitle: str,
) -> None:
    match = CONSTITUENCY_RE.match(subtitle)
    if match is None:
        raise ValueError(f"Unexpected member subtitle for {name!r}: {subtitle!r}")
    constituency = match.group("constituency").strip()

    person = context.make("Person")
    person.id = context.make_id(name, constituency)
    person.add("name", name)
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
        wikidata_id="Q17633943",
    )
    categorisation = categorise(context, position, default_is_pep=True)
    if not categorisation.is_pep:
        return
    context.emit(position)

    doc = context.fetch_html(context.data_url, headers=HEADERS, cache_days=1)
    # The responsive layout repeats each member's card several times; dedupe on
    # (name, constituency).
    seen: set[tuple[str, str]] = set()
    cards = h.xpath_elements(
        doc, '//*[contains(@class, "card")][.//h5[@class="card-title"]]'
    )
    for card in cards:
        name = HONORIFIC_RE.sub(
            "", h.element_text(h.xpath_element(card, './/h5[@class="card-title"]'))
        ).strip()
        subtitle = h.element_text(h.xpath_element(card, './/p[@class="card-text"]'))
        assert name, "Empty member name"
        key = (name, subtitle)
        if key in seen:
            continue
        seen.add(key)
        crawl_member(context, position, categorisation, name, subtitle)

    if not seen:
        raise ValueError("No member cards found on the Solomon Islands members page")
