import re
from dataclasses import dataclass
from itertools import count
from typing import Optional

from normality import squash_spaces

from zavod import Context
from zavod import helpers as h
from zavod.entity import Entity
from zavod.extract import zyte_api
from zavod.stateful.positions import PositionCategorisation, categorise
from zavod.util import Element


UNBLOCK_VALIDATOR = './/a[contains(@href, "Members/Index?ElectionId=")]'
# Terms have up to ~68 pages of eight members; this only guards against the
# pagination never terminating.
MAX_PAGES = 100

# Elections are nominally five years apart but have slipped (the 2020 election
# was held in 2021), so the years are listed rather than derived. When the site
# exposes a new term, add its election year.
ELECTION_YEARS = {
    1: "1995",
    2: "2000",
    3: "2005",
    4: "2010",
    5: "2015",
    6: "2021",
}


@dataclass(frozen=True)
class Term:
    """A parliamentary term discovered from the site's term switcher.

    The source labels terms only, so the bounds come from ``ELECTION_YEARS`` at
    year precision. ``period_end`` is ``None`` for the sitting parliament.
    """

    election_id: int
    ordinal: int
    period_start: str
    period_end: Optional[str]


def fetch_page(context: Context, url: str) -> Element:
    return zyte_api.fetch_html(
        context,
        url,
        unblock_validator=UNBLOCK_VALIDATOR,
        geolocation="et",
        # English names, party and region are only served with this cookie;
        # without it the site responds in Amharic.
        request_cookies=[
            {"name": "language", "value": "en", "domain": "www.hopr.gov.et"}
        ],
        cache_days=14,
    )


def discover_terms(context: Context, doc: Element) -> list[Term]:
    """Read the parliamentary terms from the page's term switcher, newest first.

    A term whose ordinal is not yet in ``ELECTION_YEARS`` is skipped with a
    warning, so a newly-added term surfaces for maintenance rather than being
    emitted with guessed dates.
    """
    terms: dict[int, Term] = {}
    for link in h.xpath_elements(doc, UNBLOCK_VALIDATOR):
        href = h.xpath_string(link, "./@href")
        id_match = re.search(r"ElectionId=(\d+)", href)
        # The switcher labels read like "6th Term MP"; this also excludes the
        # pagination links, which share the ElectionId href but are numbered.
        ordinal_match = re.match(r"(\d+)(?:st|nd|rd|th)\s+Term", h.element_text(link))
        if id_match is None or ordinal_match is None:
            continue
        ordinal = int(ordinal_match.group(1))
        if ordinal not in ELECTION_YEARS:
            context.log.warning(
                "Undated parliamentary term; add its election year to ELECTION_YEARS",
                ordinal=ordinal,
                href=href,
            )
            continue
        terms[ordinal] = Term(
            election_id=int(id_match.group(1)),
            ordinal=ordinal,
            period_start=ELECTION_YEARS[ordinal],
            period_end=ELECTION_YEARS.get(ordinal + 1),
        )
    return sorted(terms.values(), key=lambda term: term.ordinal, reverse=True)


def clean_value(value: Optional[str]) -> Optional[str]:
    """Trim a raw cell and drop the source's placeholders: the literal "Unknown"
    and punctuation-only cells (e.g. a lone "."), which carry no information."""
    if value is None:
        return None
    value = squash_spaces(value)
    if value == "Unknown":
        return None
    if not any(char.isalnum() for char in value):
        return None
    return value


def parse_card(card: Element) -> Optional[dict[str, Optional[str]]]:
    """Extract the fields of a single member card.

    Returns ``None`` when the card lacks the MemberId that identifies the
    member; every populated card carries one.
    """
    # Both anchors (photo and name) carry the same MemberId, so the first is enough.
    hrefs = h.xpath_strings(card, './/a[contains(@href, "MemberId=")]/@href')
    match = re.search(r"MemberId=(\d+)", hrefs[0]) if hrefs else None
    name = h.element_text(h.xpath_element(card, './/a[@class="member-name"]'))
    if match is None or not name:
        return None
    member_id = match.group(1)

    party = h.xpath_string(card, './/p[@class="member-party"]/text()')

    # The region sits in a leading <strong>; the constituency is the text after
    # the <br>, stays in Amharic even in the English view, and is sometimes blank.
    location = h.xpath_element(card, './/p[@class="member-location"]')
    region = h.xpath_string(location, "./strong/text()")
    constituency = " ".join(h.xpath_strings(location, "./br/following-sibling::text()"))

    return {
        "member_id": member_id,
        "name": name,
        "party": clean_value(party),
        "region": clean_value(region),
        "constituency": clean_value(constituency),
    }


def crawl_member(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    term: Term,
    card: dict[str, Optional[str]],
) -> None:
    member_id = card["member_id"]
    assert member_id is not None, card
    name = card["name"]
    assert name is not None, card
    clean_name = h.strip_name_titles(context, name)
    if clean_name is None:
        return

    person = context.make("Person")
    person.id = context.make_id(member_id)
    original = name if clean_name != name else None
    person.add("name", clean_name, lang="eng", original_value=original)
    person.add("political", card["party"], lang="eng")
    # Every Ethiopian national has the right to be elected to any office (FDRE
    # Constitution, 1995, Article 38(1)(c)).
    # https://www.constituteproject.org/constitution/Ethiopia_1994
    person.add("citizenship", "et")

    occupancy = h.make_occupancy(
        context,
        person,
        position,
        no_end_implies_current=term.period_end is None,
        period_start=term.period_start,
        period_end=term.period_end,
        categorisation=categorisation,
    )
    if occupancy is None:
        return
    occupancy.add("constituency", [card["region"], card["constituency"]])
    context.emit(occupancy)
    context.emit(person)


def crawl_term(
    context: Context,
    position: Entity,
    categorisation: PositionCategorisation,
    term: Term,
) -> None:
    """Paginate through one term's member list and emit its members."""
    for page in count(1):
        if page > MAX_PAGES:
            raise ValueError(f"HoPR {term.ordinal} term exceeded the page cap")
        url = f"{context.data_url}?ElectionId={term.election_id}&page={page}"
        doc = fetch_page(context, url)
        cards = h.xpath_elements(doc, './/div[@class="member-card"]')
        # Pages past the last one render no cards; that ends the pagination.
        if not cards:
            break
        for card in cards:
            data = parse_card(card)
            if data is not None:
                crawl_member(context, position, categorisation, term, data)
    context.log.info("Crawled term", ordinal=term.ordinal, election_id=term.election_id)


def crawl(context: Context) -> None:
    position = h.make_position(
        context,
        name="Member of the House of Peoples' Representatives of Ethiopia",
        country="et",
        wikidata_id="Q21328614",
        lang="eng",
    )
    categorisation = categorise(context, position)
    if not categorisation.is_pep:
        return
    context.emit(position)

    # The bare members index lists the term switcher and defaults to the sitting
    # parliament; use it to discover which terms the site exposes.
    terms = discover_terms(context, fetch_page(context, context.data_url))
    cutoff_year = int(h.earliest_term_start(["gov.national", "gov.legislative"])[:4])
    for term in terms:
        if term.period_end is not None and int(term.period_end) < cutoff_year:
            continue
        crawl_term(context, position, categorisation, term)
